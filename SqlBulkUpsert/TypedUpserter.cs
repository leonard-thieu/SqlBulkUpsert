using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SqlBulkUpsert
{
    public sealed class TypedUpserter<T>
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

            using (var command = SqlCommandAdapter.FromConnection(connection))
            {
                command.CommandText = $@"TRUNCATE TABLE [{columnMappings.TableName}];";
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            using (var dataReader = new TypedDataReader<T>(columnMappings, items))
            {
                await BulkCopyAsync(connection, dataReader, cancellationToken).ConfigureAwait(false);

                return items.Count();
            }
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
                await BulkCopyAsync(connection, dataReader, cancellationToken).ConfigureAwait(false);

                var targetTableSchema = await SqlTableSchema.LoadFromDatabaseAsync(connection, columnMappings.TableName, cancellationToken).ConfigureAwait(false);
                return await tempTable.MergeAsync(targetTableSchema, updateOnMatch, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task BulkCopyAsync(
            SqlConnection connection,
            IDataReader data,
            CancellationToken cancellationToken)
        {
            using (var copy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, null))
            {
                copy.BulkCopyTimeout = 0;
                copy.DestinationTableName = columnMappings.TableName;

                foreach (var columnName in columnMappings.Keys)
                {
                    copy.ColumnMappings.Add(columnName, columnName);
                }

                await copy.WriteToServerAsync(data, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}