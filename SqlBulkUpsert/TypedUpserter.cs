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

        private readonly ColumnMappings<T> columnMappings;

        public async Task<int> InsertAsync(
            SqlConnection connection,
            IEnumerable<T> items,
            CancellationToken cancellationToken = default)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            var viewName = columnMappings.TableName;

            var stagingTableName = $"{viewName}_A";
            var activeTableName = $"{viewName}_B";
            var count = await connection.CountAsync(stagingTableName, cancellationToken).ConfigureAwait(false);
            if (count != 0)
            {
                stagingTableName = $"{viewName}_B";
                activeTableName = $"{viewName}_A";
            }

            await connection.DisableNonclusteredIndexesAsync(stagingTableName, cancellationToken).ConfigureAwait(false);
            // Cannot assume that the staging table is empty even though it's truncated afterwards.
            // This can happen when initially working with a database that was modified by older code. Older code 
            // truncated at the beginning instead of after.
            await connection.TruncateTableAsync(stagingTableName, cancellationToken).ConfigureAwait(false);
            using (var dataReader = new TypedDataReader<T>(columnMappings, items))
            {
                await BulkCopyAsync(connection, stagingTableName, dataReader, cancellationToken).ConfigureAwait(false);
            }
            await connection.RebuildNonclusteredIndexesAsync(stagingTableName, cancellationToken).ConfigureAwait(false);

            var schema = await connection.SelectTableSchemaAsync(stagingTableName, cancellationToken).ConfigureAwait(false);
            await connection.SwitchTableAsync(viewName, stagingTableName, schema.Columns, cancellationToken).ConfigureAwait(false);
            // Active table is now the new staging table
            await connection.TruncateTableAsync(activeTableName, cancellationToken).ConfigureAwait(false);

            return items.Count();
        }

        public async Task<int> UpsertAsync(
            SqlConnection connection,
            IEnumerable<T> items,
            bool updateWhenMatched,
            CancellationToken cancellationToken = default)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            var baseTableName = columnMappings.TableName;
            var tempTableName = $"#{baseTableName}";
            await connection.SelectIntoTemporaryTableAsync(baseTableName, tempTableName, cancellationToken).ConfigureAwait(false);
            using (var dataReader = new TypedDataReader<T>(columnMappings, items))
            {
                await BulkCopyAsync(connection, tempTableName, dataReader, cancellationToken).ConfigureAwait(false);
            }
            var targetTableSchema = await connection.SelectTableSchemaAsync(baseTableName, cancellationToken).ConfigureAwait(false);

            return await connection.MergeAsync(tempTableName, targetTableSchema, updateWhenMatched, cancellationToken).ConfigureAwait(false);
        }

        private async Task BulkCopyAsync(
            SqlConnection connection,
            string tableName,
            IDataReader data,
            CancellationToken cancellationToken)
        {
            using (var sqlBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock & SqlBulkCopyOptions.KeepNulls, null))
            {
                sqlBulkCopy.BulkCopyTimeout = 0;
                sqlBulkCopy.DestinationTableName = tableName;

                foreach (var columnName in columnMappings.Columns)
                {
                    sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
                }

                await sqlBulkCopy.WriteToServerAsync(data, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}