using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;
using SqlBulkUpsert.Tests.Properties;

namespace SqlBulkUpsert.Tests
{
    static class DatabaseHelper
    {
        public static void CreateDatabase() => ExecuteCommands(Resources.CreateDatabase);

        public static void DropDatabase() => ExecuteCommands(Resources.DropDatabase);

        /// <summary>
        /// Execute some SQL against the database
        /// </summary>
        /// <param name="sqlCommandText">SQL containing one or multiple command separated by \r\nGO\r\n</param>
        static void ExecuteCommands(string sqlCommandText)
        {
            using (var connection = CreateAndOpenConnection())
            using (var command = connection.CreateCommand())
            {
                var batches = Regex.Split(sqlCommandText, "(?:\r?\n)?GO(?:\r?\n)?", RegexOptions.IgnoreCase);
                foreach (var batch in batches.Where(b => !string.IsNullOrWhiteSpace(b)))
                {
                    command.CommandText = batch;
                    command.ExecuteNonQuery();
                }
            }
        }

        public static SqlConnection CreateAndOpenConnection(string databaseName = null)
        {
            var connectionString =
                Environment.GetEnvironmentVariable("SqlBulkUpsertTestConnectionString", EnvironmentVariableTarget.Machine) ??
                ConfigurationManager.ConnectionStrings["SqlBulkUpsertTestConnectionString"].ConnectionString;

            var sqlConnection = new SqlConnection(connectionString);
            sqlConnection.Open();

            if (databaseName != null)
            {
                using (var command = sqlConnection.CreateCommand())
                {
                    command.CommandText = $"USE [{databaseName}];";
                    command.ExecuteNonQuery();
                }
            }

            return sqlConnection;
        }
    }
}