﻿using System;
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

                sqlCommand.CommandText = $@"
USE [{connection.Database}];

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

                    return await LoadFromReaderAsync(tableName, sqlDataReader, cancellationToken).ConfigureAwait(false);
                }
            }
        }

        internal static async Task<SqlTableSchema> LoadFromReaderAsync(
            string tableName,
            DbDataReader sqlDataReader,
            CancellationToken cancellationToken)
        {
            if (sqlDataReader == null)
                throw new ArgumentNullException(nameof(sqlDataReader));

            var columns = new List<Column>();
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
            IEnumerable<Column> columns,
            IEnumerable<string> primaryKeyColumnNames)
        {
            if (columns == null)
                throw new ArgumentNullException(nameof(columns));
            if (!columns.Any())
                throw new ArgumentException();
            if (primaryKeyColumnNames == null)
                throw new ArgumentNullException(nameof(primaryKeyColumnNames));

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
        public ICollection<Column> Columns { get; } = new Collection<Column>();
        public ICollection<Column> PrimaryKeyColumns { get; } = new Collection<Column>();

        public string ToCreateTableCommandText() => $"CREATE TABLE [{TableName}] ({Columns.ToColumnDefinitionListString()});";

        public string ToDropTableCommandText() => $"DROP TABLE [{TableName}];";
    }
}