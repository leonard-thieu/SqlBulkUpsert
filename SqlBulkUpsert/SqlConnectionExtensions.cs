using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace SqlBulkUpsert
{
    static class SqlConnectionExtensions
    {
        public static async Task TruncateTable(
            this SqlConnection connection,
            string tableName,
            CancellationToken cancellationToken)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));

            using (var command = SqlCommandAdapter.FromConnection(connection))
            {
                command.CommandText = $"TRUNCATE TABLE [{tableName}];";

                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public static Task DisableNonclusteredIndexes(
            this SqlConnection connection,
            string tableName,
            CancellationToken cancellationToken)
        {
            return AlterNonclusteredIndexes(connection, tableName, "DISABLE", cancellationToken);
        }

        public static Task RebuildNonclusteredIndexes(
            this SqlConnection connection,
            string tableName,
            CancellationToken cancellationToken)
        {
            return AlterNonclusteredIndexes(connection, tableName, "REBUILD", cancellationToken);
        }

        static async Task AlterNonclusteredIndexes(
            this SqlConnection connection,
            string tableName,
            string action,
            CancellationToken cancellationToken)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));
            switch (action)
            {
                case "DISABLE":
                case "REBUILD":
                    break;
                default:
                    throw new ArgumentException($"'{action}' is not a valid action for altering indexes.", nameof(action));
            }

            using (var command = SqlCommandAdapter.FromConnection(connection))
            {
                command.CommandText = $@"
DECLARE @sql AS VARCHAR(MAX)='';

SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' {action};' + CHAR(13) + CHAR(10)
FROM sys.indexes
JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id
WHERE sys.indexes.type_desc = 'NONCLUSTERED'
  AND sys.objects.type_desc = 'USER_TABLE'
  AND sys.objects.name = @tableName;

EXEC(@sql);";
                command.Parameters.Add("@tableName", SqlDbType.VarChar).Value = tableName;

                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
