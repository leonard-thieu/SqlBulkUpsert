using System;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using SqlBulkUpsert.Tests.Properties;

namespace SqlBulkUpsert.Tests
{
    internal static class DatabaseHelper
    {
        public static Task CreateDatabaseAsync() => ExecuteCommandsAsync(Resources.CreateDatabase);

        public static Task DropDatabaseAsync() => ExecuteCommandsAsync(Resources.DropDatabase);

        /// <summary>
        /// Execute some SQL against the database
        /// </summary>
        /// <param name="sqlCommandText">SQL containing one or multiple command separated by \r\nGO\r\n</param>
        private static async Task ExecuteCommandsAsync(string sqlCommandText)
        {
            using (var connection = await CreateAndOpenConnectionAsync().ConfigureAwait(false))
            using (var command = connection.CreateCommand())
            {
                var batches = Regex.Split(sqlCommandText, "(?:\r?\n)?GO(?:\r?\n)?", RegexOptions.IgnoreCase);
                foreach (var batch in batches.Where(b => !string.IsNullOrWhiteSpace(b)))
                {
                    command.CommandText = batch;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
        }

        public static async Task<SqlConnection> CreateAndOpenConnectionAsync(string databaseName = null)
        {
            var connectionString = GetConnectionString();

            var sqlConnection = new SqlConnection(connectionString);
            await sqlConnection.OpenAsync().ConfigureAwait(false);

            if (databaseName != null)
            {
                await sqlConnection.UseAsync(databaseName).ConfigureAwait(false);
            }

            return sqlConnection;
        }

        public static string GetConnectionString()
        {
            return Environment.GetEnvironmentVariable("SqlBulkUpsertTestConnectionString", EnvironmentVariableTarget.Machine) ??
                "Data Source=localhost;Integrated Security=SSPI;";
        }
    }
}