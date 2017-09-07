using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SqlBulkUpsert
{
    public sealed class TypedUpserter<T> : ITypedUpserter<T>
    {
        public TypedUpserter(ColumnMappings<T> columnMappings)
        {
            this.columnMappings = columnMappings ?? throw new ArgumentNullException(nameof(columnMappings));
        }

        readonly ColumnMappings<T> columnMappings;

        public async Task<int> InsertAsync(
            SqlConnection connection,
            IEnumerable<T> items,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            var viewName = columnMappings.TableName;

            var stagingTableName = $"{viewName}_A";
            var activeTableName = $"{viewName}_B";
            var rowCount = await connection.GetRowCountAsync(stagingTableName, cancellationToken).ConfigureAwait(false);
            if (rowCount != 0)
            {
                stagingTableName = $"{viewName}_B";
                activeTableName = $"{viewName}_A";
            }

            await connection.DisableNonclusteredIndexesAsync(stagingTableName, cancellationToken).ConfigureAwait(false);
            using (var dataReader = new TypedDataReader<T>(columnMappings, items))
            {
                await BulkCopyAsync(connection, stagingTableName, dataReader, cancellationToken).ConfigureAwait(false);
            }
            await connection.RebuildNonclusteredIndexesAsync(stagingTableName, cancellationToken).ConfigureAwait(false);

            await connection.SwitchTableAsync(viewName, stagingTableName, cancellationToken).ConfigureAwait(false);
            await connection.TruncateTableAsync(activeTableName, cancellationToken).ConfigureAwait(false);

            return items.Count();
        }

        public async Task<int> UpsertAsync(
            SqlConnection connection,
            IEnumerable<T> items,
            bool updateOnMatch,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            using (var tempTable = await TemporaryTable.CreateAsync(connection, columnMappings.TableName, cancellationToken).ConfigureAwait(false))
            using (var dataReader = new TypedDataReader<T>(columnMappings, items))
            {
                await BulkCopyAsync(connection, columnMappings.TableName, dataReader, cancellationToken).ConfigureAwait(false);

                var targetTableSchema = await SqlTableSchema.LoadFromDatabaseAsync(connection, columnMappings.TableName, cancellationToken).ConfigureAwait(false);

                return await tempTable.MergeAsync(targetTableSchema, updateOnMatch, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task BulkCopyAsync(
            SqlConnection connection,
            string tableName,
            IDataReader data,
            CancellationToken cancellationToken)
        {
            using (var sqlBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, null))
            {
                sqlBulkCopy.BulkCopyTimeout = 0;
                sqlBulkCopy.DestinationTableName = tableName;

                foreach (var columnName in columnMappings.Keys)
                {
                    sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
                }

                await sqlBulkCopy.WriteToServerAsync(data, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}