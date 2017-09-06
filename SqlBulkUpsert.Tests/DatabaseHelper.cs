using System;
using System.Configuration;
using System.Data.SqlClient;
using SqlBulkUpsert.Tests.Properties;

namespace SqlBulkUpsert.Tests
{
    static class DatabaseHelper
    {
        public static void RefreshSchema()
        {
            ExecuteCommands(Resources.CreateDatabase);
        }

        public static void DropDatabase()
        {
            ExecuteCommands(Resources.DropDatabase);
        }

        /// <summary>
        /// Execute some SQL against the database
        /// </summary>
        /// <param name="sqlCommandText">SQL containing one or multiple command separated by \r\nGO\r\n</param>
        static void ExecuteCommands(string sqlCommandText)
        {
            using (var connection = CreateAndOpenConnection())
            {
                connection.ExecuteCommands(sqlCommandText);
            }
        }

        public static SqlConnection CreateAndOpenConnection()
        {
            var connectionString =
                Environment.GetEnvironmentVariable("SqlBulkUpsertTestConnectionString", EnvironmentVariableTarget.Machine) ??
                ConfigurationManager.ConnectionStrings["SqlBulkUpsertTestConnectionString"].ConnectionString;
            var sqlConnection = new SqlConnection(connectionString);
            sqlConnection.Open();

            return sqlConnection;
        }
    }
}