using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SqlBulkUpsert
{
    sealed class SqlTableSchema
    {
        [SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        public static async Task<SqlTableSchema> LoadFromDatabaseAsync(
            SqlConnection connection,
            string tableName,
            CancellationToken cancellationToken)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));
            if (tableName == null)
                throw new ArgumentNullException(nameof(tableName));

            using (var sqlCommand = connection.CreateCommand())
            {
                const string TableNameParam = "@tableName";

                sqlCommand.CommandText = $@"USE [{connection.Database}];

-- Check table exists
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
                sqlCommand.Parameters.Add(TableNameParam, SqlDbType.VarChar).Value = tableName;

                using (var sqlDataReader = await sqlCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (!await sqlDataReader.ReadAsync(cancellationToken).ConfigureAwait(false))
                    {
                        throw new InvalidOperationException("Table not found.");
                    }

                    await sqlDataReader.NextResultAsync(cancellationToken).ConfigureAwait(false);

                    return await LoadFromReaderAsync(sqlDataReader, tableName, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        internal static async Task<SqlTableSchema> LoadFromReaderAsync(
            DbDataReader sqlDataReader,
            string tableName,
            CancellationToken cancellationToken)
        {
            var columns = new List<ColumnBase>();
            var primaryKeyColumns = new List<string>();

            while (await sqlDataReader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var column = ColumnFactory.CreateFromReader(sqlDataReader);
                columns.Add(column);
            }

            await sqlDataReader.NextResultAsync(cancellationToken).ConfigureAwait(false);

            while (await sqlDataReader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var columnName = (string)sqlDataReader["COLUMN_NAME"];
                primaryKeyColumns.Add(columnName);
            }

            return new SqlTableSchema(tableName, columns, primaryKeyColumns);
        }

        internal SqlTableSchema(
            string tableName,
            IEnumerable<ColumnBase> columns,
            IEnumerable<string> primaryKeyColumnNames)
        {
            TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));

            foreach (var column in columns)
            {
                Columns.Add(column);
            }

            foreach (var columnName in primaryKeyColumnNames)
            {
                var column = Columns.Single(c => c.Name == columnName);
                PrimaryKeyColumns.Add(column);
            }
        }

        public string TableName { get; }
        public ICollection<ColumnBase> Columns { get; } = new Collection<ColumnBase>();
        public ICollection<ColumnBase> PrimaryKeyColumns { get; } = new Collection<ColumnBase>();
    }
}