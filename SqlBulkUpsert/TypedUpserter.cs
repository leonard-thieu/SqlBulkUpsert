using System;
using System.Collections.Generic;
using System.Data;
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
            ISqlBulkCopy sqlBulkCopy,
            IEnumerable<T> items,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (sqlBulkCopy == null)
                throw new ArgumentNullException(nameof(sqlBulkCopy));
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            using (var command = SqlCommandAdapter.FromConnection(sqlBulkCopy.Connection))
            {
                command.CommandText = $@"TRUNCATE TABLE [{columnMappings.TableName}];";
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            using (var dataReader = new TypedDataReader<T>(columnMappings, items))
            {
                await BulkCopyAsync(sqlBulkCopy, dataReader, cancellationToken).ConfigureAwait(false);

                return items.Count();
            }
        }

        public async Task<int> UpsertAsync(
            ISqlBulkCopy sqlBulkCopy,
            IEnumerable<T> items,
            bool updateOnMatch,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (sqlBulkCopy == null)
                throw new ArgumentNullException(nameof(sqlBulkCopy));
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            var connection = sqlBulkCopy.Connection;
            using (var tempTable = await TemporaryTable.CreateAsync(connection, columnMappings.TableName, cancellationToken).ConfigureAwait(false))
            using (var dataReader = new TypedDataReader<T>(columnMappings, items))
            {
                await BulkCopyAsync(sqlBulkCopy, dataReader, cancellationToken).ConfigureAwait(false);

                var targetTableSchema = await SqlTableSchema.LoadFromDatabaseAsync(connection, columnMappings.TableName, cancellationToken).ConfigureAwait(false);

                return await tempTable.MergeAsync(targetTableSchema, updateOnMatch, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task BulkCopyAsync(
            ISqlBulkCopy sqlBulkCopy,
            IDataReader data,
            CancellationToken cancellationToken)
        {
            sqlBulkCopy.BulkCopyTimeout = 0;
            sqlBulkCopy.DestinationTableName = columnMappings.TableName;

            foreach (var columnName in columnMappings.Keys)
            {
                sqlBulkCopy.ColumnMappings.Add(columnName, columnName);
            }

            await sqlBulkCopy.WriteToServerAsync(data, cancellationToken).ConfigureAwait(false);
        }
    }
}