using System.Data;
using System.Linq;
using System.Text.RegularExpressions;

namespace SqlBulkUpsert.Tests
{
    static class IDbConnectionExtensions
    {
        /// <summary>
        /// Execute multiple non-query commands against a connection.
        /// </summary>
        public static void ExecuteCommands(this IDbConnection connection, string script)
        {
            using (var command = connection.CreateCommand())
            {
                var batches = Regex.Split(script, "(?:\r?\n)?GO(?:\r?\n)?", RegexOptions.IgnoreCase);
                foreach (var batch in batches.Where(b => !string.IsNullOrWhiteSpace(b)))
                {
                    command.CommandText = batch;
                    command.ExecuteNonQuery();
                }
            }
        }

        public static void Use(this IDbConnection connection, string database)
        {
            connection.ExecuteCommands($"USE [{database}];");
        }
    }
}