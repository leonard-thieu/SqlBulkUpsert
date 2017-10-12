using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SqlBulkUpsert
{
    static class SqlConnectionExtensions
    {
        #region Use

        public static async Task UseAsync(
            this SqlConnection connection,
            string databaseName,
            CancellationToken cancellationToken = default)
        {
            if (databaseName == null)
                throw new ArgumentNullException(nameof(databaseName));

            using (var command = GetUseCommand(connection, databaseName))
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        internal static SqlCommandAdapter GetUseCommand(
            SqlConnection connection,
            string databaseName)
        {
            var command = SqlCommandAdapter.FromConnection(connection);

            command.CommandText = $"USE [{databaseName}];";

            return command;
        }

        #endregion

        #region SelectTableSchema

        public static async Task<SqlTableSchema> SelectTableSchemaAsync(
            this SqlConnection connection,
            string tableName,
            CancellationToken cancellationToken = default)
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));

            using (var command = GetSelectTableSchemaCommand(connection, tableName))
            using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
            {
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    throw new InvalidOperationException("Table not found.");
                }

                await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);

                return await ReadTableSchemaAsync(reader, tableName, cancellationToken).ConfigureAwait(false);
            }
        }

        internal static SqlCommandAdapter GetSelectTableSchemaCommand(
            SqlConnection connection,
            string tableName)
        {
            const string TableNameParam = "@tableName";

            var command = SqlCommandAdapter.FromConnection(connection);

            command.CommandText = $@"-- Check table exists
SELECT *
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = {TableNameParam};

-- Get column schema information for table (need this to create our temp table)
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = {TableNameParam};

-- Identifies the columns making up the primary key (do we use this for our match?)
SELECT kcu.COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
    AND CONSTRAINT_TYPE = 'PRIMARY KEY'
WHERE kcu.TABLE_NAME = {TableNameParam};";
            command.Parameters.Add(TableNameParam, SqlDbType.VarChar).Value = tableName;

            return command;
        }

        internal static async Task<SqlTableSchema> ReadTableSchemaAsync(
            DbDataReader reader,
            string tableName,
            CancellationToken cancellationToken)
        {
            var columns = new List<ColumnBase>();
            var primaryKeyColumns = new List<string>();

            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var column = ColumnFactory.CreateFromReader(reader);
                columns.Add(column);
            }

            await reader.NextResultAsync(cancellationToken).ConfigureAwait(false);

            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var columnName = (string)reader["COLUMN_NAME"];
                primaryKeyColumns.Add(columnName);
            }

            return new SqlTableSchema(tableName, columns, primaryKeyColumns);
        }

        #endregion

        #region Count

        public static async Task<int> CountAsync(
            this SqlConnection connection,
            string tableName,
            CancellationToken cancellationToken = default)
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));

            using (var command = GetCountCommand(connection, tableName))
            {
                var scalar = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

                return Convert.ToInt32(scalar);
            }
        }

        internal static SqlCommandAdapter GetCountCommand(
            SqlConnection connection,
            string tableName)
        {
            var command = SqlCommandAdapter.FromConnection(connection);

            command.CommandText = $@"SELECT Count(*) 
FROM [{tableName}];";

            return command;
        }

        #endregion

        #region SwitchTable

        public static async Task SwitchTableAsync(
            this SqlConnection connection,
            string viewName,
            string tableName,
            ICollection<ColumnBase> columns,
            CancellationToken cancellationToken = default)
        {
            if (viewName == null)
                throw new ArgumentNullException(nameof(viewName));
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));
            if (columns == null)
                throw new ArgumentNullException(nameof(columns));

            using (var command = GetSwitchTableCommand(connection, viewName, tableName, columns))
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        internal static SqlCommandAdapter GetSwitchTableCommand(
            SqlConnection connection,
            string viewName,
            string tableName,
            ICollection<ColumnBase> columns)
        {
            var command = SqlCommandAdapter.FromConnection(connection);

            command.CommandText = $@"ALTER VIEW [{viewName}]
AS

SELECT {columns.ToSelectListString()}
FROM [{tableName}];";

            return command;
        }

        #endregion

        #region TruncateTable

        public static async Task TruncateTableAsync(
            this SqlConnection connection,
            string tableName,
            CancellationToken cancellationToken = default)
        {
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));

            using (var command = GetTruncateTableCommand(connection, tableName))
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        internal static SqlCommandAdapter GetTruncateTableCommand(
            SqlConnection connection,
            string tableName)
        {
            var command = SqlCommandAdapter.FromConnection(connection);

            command.CommandText = $"TRUNCATE TABLE [{tableName}];";

            return command;
        }

        #endregion

        #region SelectIntoTemporaryTable

        public static async Task SelectIntoTemporaryTableAsync(
            this SqlConnection connection,
            string baseTableName,
            string tempTableName,
            CancellationToken cancellationToken = default)
        {
            if (baseTableName == null)
                throw new ArgumentNullException(nameof(baseTableName));
            if (tempTableName == null)
                throw new ArgumentNullException(nameof(tempTableName));

            using (var command = GetSelectIntoTemporaryTableCommand(connection, baseTableName, tempTableName))
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        internal static SqlCommandAdapter GetSelectIntoTemporaryTableCommand(
            SqlConnection connection,
            string baseTableName,
            string tempTableName)
        {
            var command = SqlCommandAdapter.FromConnection(connection);

            command.CommandText = $"SELECT TOP 0 * INTO [{tempTableName}] FROM [{baseTableName}];";

            return command;
        }

        #endregion

        #region Merge

        public static async Task<int> MergeAsync(
            this SqlConnection connection,
            string tableSource,
            SqlTableSchema targetTableSchema,
            bool updateWhenMatched,
            CancellationToken cancellationToken = default)
        {
            if (tableSource == null)
                throw new ArgumentNullException(nameof(tableSource));
            if (targetTableSchema == null)
                throw new ArgumentNullException(nameof(targetTableSchema));

            using (var command = GetMergeCommand(connection, tableSource, targetTableSchema, updateWhenMatched))
            {
                return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        internal static SqlCommandAdapter GetMergeCommand(
            SqlConnection connection,
            string tableSource,
            SqlTableSchema targetTableSchema,
            bool updateWhenMatched)
        {
            var command = SqlCommandAdapter.FromConnection(connection);

            var targetTable = targetTableSchema.TableName;
            var mergeSearchCondition = GetMergeSearchCondition();
            var setClause = GetSetClause();
            var columnList = GetValuesList();
            var valuesList = GetValuesList();

            var sb = new StringBuilder();

            sb.AppendLine($"MERGE INTO [{targetTable}] AS [Target]");
            sb.AppendLine($"USING [{tableSource}] AS [Source]");
            sb.AppendLine($"    ON ({mergeSearchCondition})");

            if (updateWhenMatched)
            {
                sb.AppendLine($"WHEN MATCHED");
                sb.AppendLine($"    THEN");
                sb.AppendLine($"        UPDATE");
                sb.AppendLine($"        SET {setClause}");
            }

            sb.AppendLine($"WHEN NOT MATCHED");
            sb.AppendLine($"    THEN");
            sb.AppendLine($"        INSERT ({columnList})");
            sb.AppendLine($"        VALUES ({valuesList});");

            command.CommandText = sb.ToString();

            return command;

            string GetMergeSearchCondition()
            {
                var columns = from c in targetTableSchema.PrimaryKeyColumns
                              let column = c.ToSelectListString()
                              select $"[Target].{column} = [Source].{column}";

                return string.Join(" AND ", columns);
            }

            string GetSetClause()
            {
                // Exclude primary key and identity columns
                var columns = from c in targetTableSchema.Columns.Except(targetTableSchema.PrimaryKeyColumns)
                              let column = c.ToSelectListString()
                              select $"[Target].{column} = [Source].{column}";

                return string.Join(",\r\n            ", columns);
            }

            string GetValuesList() => targetTableSchema.Columns.ToSelectListString();
        }

        #endregion

        #region AlterNonclusteredIndexes

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

            using (var command = GetAlterNonclusteredIndexesCommand(connection, tableName, action))
            {
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        internal static SqlCommandAdapter GetAlterNonclusteredIndexesCommand(
            SqlConnection connection,
            string tableName,
            string action)
        {
            var command = SqlCommandAdapter.FromConnection(connection);

            command.CommandText = $@"DECLARE @sql AS VARCHAR(MAX)='';

SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' {action};' + CHAR(13) + CHAR(10)
FROM sys.indexes
JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id
WHERE sys.indexes.type_desc = 'NONCLUSTERED'
  AND sys.objects.type_desc = 'USER_TABLE'
  AND sys.objects.name = @tableName;

EXEC(@sql);";
            command.Parameters.Add("@tableName", SqlDbType.VarChar).Value = tableName;

            return command;
        }

        #endregion
    }
}
