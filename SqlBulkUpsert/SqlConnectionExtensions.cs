using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace SqlBulkUpsert
{
    static class SqlConnectionExtensions
    {
        public static async Task UseAsync(
            this SqlConnection connection,
            string databaseName,
            CancellationToken cancellationToken = default)
        {
            if (databaseName == null)
                throw new ArgumentNullException(nameof(databaseName));

            using (var command = SqlCommandAdapter.FromConnection(connection))
            {
                command.CommandText = $"USE [{databaseName}];";

                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public static async Task<int> GetRowCountAsync(
            this SqlConnection connection,
            string tableName,
            CancellationToken cancellationToken = default)
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));

            using (var command = SqlCommandAdapter.FromConnection(connection))
            {
                command.CommandText = $@"SELECT Count(*) 
FROM [{tableName}];";

                var scalar = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

                return Convert.ToInt32(scalar);
            }
        }

        public static async Task SwitchTableAsync(
            this SqlConnection connection,
            string viewName,
            string tableName,
            CancellationToken cancellationToken = default)
        {
            if (viewName == null)
                throw new ArgumentNullException(nameof(viewName));
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));

            var tableSchema = await SqlTableSchema.LoadFromDatabaseAsync(connection, tableName, cancellationToken).ConfigureAwait(false);

            using (var command = SqlCommandAdapter.FromConnection(connection))
            {
                command.CommandText = $@"ALTER VIEW [{viewName}]
AS

SELECT {tableSchema.Columns.ToSelectListString()}
FROM [{tableName}];";

                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public static async Task TruncateTableAsync(
            this SqlConnection connection,
            string tableName,
            CancellationToken cancellationToken = default)
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));

            using (var command = SqlCommandAdapter.FromConnection(connection))
            {
                command.CommandText = $"TRUNCATE TABLE [{tableName}];";

                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        public static Task DisableNonclusteredIndexesAsync(
            this SqlConnection connection,
            string tableName,
            CancellationToken cancellationToken = default)
        {
            return AlterNonclusteredIndexesAsync(connection, tableName, "DISABLE", cancellationToken);
        }

        public static Task RebuildNonclusteredIndexesAsync(
            this SqlConnection connection,
            string tableName,
            CancellationToken cancellationToken = default)
        {
            return AlterNonclusteredIndexesAsync(connection, tableName, "REBUILD", cancellationToken);
        }

        static async Task AlterNonclusteredIndexesAsync(
            this SqlConnection connection,
            string tableName,
            string action,
            CancellationToken cancellationToken = default)
        {
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
                command.CommandText = $@"DECLARE @sql AS VARCHAR(MAX)='';

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
