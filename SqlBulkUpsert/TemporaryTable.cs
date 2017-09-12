using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace SqlBulkUpsert
{
    sealed class TemporaryTable : IDisposable
    {
        public static async Task<TemporaryTable> CreateAsync(
            SqlConnection connection,
            string targetTableName,
            CancellationToken cancellationToken)
        {
            var table = new TemporaryTable(connection, targetTableName);

            using (var command = SqlCommandAdapter.FromConnection(connection))
            {
                command.CommandText = $"SELECT TOP 0 * INTO [{table.Name}] FROM [{targetTableName}];";
                await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }

            return table;
        }

        TemporaryTable(SqlConnection connection, string targetTableName)
        {
            Name = $"#{targetTableName}";

            this.connection = connection;
            this.targetTableName = targetTableName;
        }

        readonly SqlConnection connection;
        readonly string targetTableName;

        public string Name { get; }

        public async Task<int> MergeAsync(
            SqlTableSchema targetTableSchema,
            bool updateWhenMatched,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            using (var command = SqlCommandAdapter.FromConnection(connection))
            {
                var mergeCommand = new MergeCommand(Name, targetTableSchema, updateWhenMatched);
                command.CommandText = mergeCommand.ToString();

                return await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        #region IDisposable Members

        bool disposed;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        void Dispose(bool disposing)
        {
            if (disposed)
                return;

            if (disposing)
            {
                // Drops non-temporary "temporary" tables
                if (Name != null && !Name.StartsWith("#", StringComparison.OrdinalIgnoreCase))
                {
                    using (var command = SqlCommandAdapter.FromConnection(connection))
                    {
                        command.CommandText = $"DROP TABLE [{Name}];";
                        command.ExecuteNonQuery();
                    }
                }
            }

            disposed = true;
        }

        #endregion
    }
}