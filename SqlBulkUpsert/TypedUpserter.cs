using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using static SqlBulkUpsert.Util;

namespace SqlBulkUpsert
{
    public sealed class TypedUpserter<T>
    {
        public TypedUpserter(SqlTableSchema targetTableSchema, ColumnMappings<T> columnMappings)
        {
            this.targetTableSchema = targetTableSchema ?? throw new ArgumentNullException(nameof(targetTableSchema));
            this.columnMappings = columnMappings ?? throw new ArgumentNullException(nameof(columnMappings));
        }

        readonly SqlTableSchema targetTableSchema;
        readonly ColumnMappings<T> columnMappings;

        public async Task<int> InsertAsync(
            SqlConnection connection,
            IEnumerable<T> items)
        {
            var cancellationToken = CancellationToken.None;

            using (var command = SqlCommandAdapter.FromConnection(connection))
            {
                command.CommandText = Invariant("TRUNCATE TABLE {0};", targetTableSchema.TableName);
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            using (var dataReader = new TypedDataReader<T>(columnMappings, items))
            {
                await BulkCopyAsync(connection, targetTableSchema.TableName, dataReader, cancellationToken).ConfigureAwait(false);

                return items.Count();
            }
        }

        public async Task<int> UpsertAsync(
            SqlConnection connection,
            IEnumerable<T> items,
            bool updateOnMatch,
            CancellationToken cancellationToken)
        {
            using (var tempTable = await TemporaryTable.CreateAsync(connection, targetTableSchema.TableName, cancellationToken).ConfigureAwait(false))
            using (var dataReader = new TypedDataReader<T>(columnMappings, items))
            {
                await BulkCopyAsync(connection, tempTable.Name, dataReader, cancellationToken).ConfigureAwait(false);

                return await tempTable.MergeAsync(targetTableSchema, updateOnMatch, cancellationToken).ConfigureAwait(false);
            }
        }

        async Task BulkCopyAsync(
            SqlConnection connection,
            string tableName,
            IDataReader data,
            CancellationToken cancellationToken)
        {
            using (var copy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, null))
            {
                foreach (var columnName in columnMappings.Keys)
                {
                    copy.ColumnMappings.Add(columnName, columnName);
                }

                copy.BulkCopyTimeout = 0;
                copy.DestinationTableName = tableName;

                await copy.WriteToServerAsync(data, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}