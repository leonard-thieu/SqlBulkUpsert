using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace SqlBulkUpsert
{
    /// <summary>
    /// Wraps an instance of <see cref="SqlCommand"/> to provide more detailed error information.
    /// </summary>
    sealed class SqlCommandAdapter : IDisposable
    {
        /// <summary>
        /// Creates and returns an instance of <see cref="SqlCommandAdapter"/> that wraps a <see cref="SqlCommand"/> that is
        /// associated with the <see cref="SqlConnection"/>.
        /// </summary>
        /// <param name="connection">The <see cref="ISqlConnection"/> to create the command for.</param>
        /// <returns>An instance of <see cref="SqlCommandAdapter"/> that wraps a <see cref="SqlCommand"/>.</returns>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="connection"/> is null.
        /// </exception>
        public static SqlCommandAdapter FromConnection(SqlConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            return new SqlCommandAdapter(connection.CreateCommand());
        }

        SqlCommandAdapter(SqlCommand command)
        {
            this.command = command;
        }

        readonly SqlCommand command;

        /// <summary>
        /// Gets or sets the Transact-SQL statement, table name or stored procedure to execute at the data source.
        /// </summary>
        public string CommandText
        {
            get
            {
                if (disposed)
                    throw new ObjectDisposedException(GetType().Name);

                return command.CommandText;
            }
            set
            {
                if (disposed)
                    throw new ObjectDisposedException(GetType().Name);

                command.CommandText = value;
            }
        }

        /// <summary>
        /// Executes a Transact-SQL statement against the connection and returns the number of rows affected.
        /// </summary>
        /// <returns>The number of rows affected.</returns>
        public int ExecuteNonQuery()
        {
            if (disposed)
                throw new ObjectDisposedException(GetType().Name);

            try
            {
                return command.ExecuteNonQuery();
            }
            catch (SqlException ex)
            {
                throw new SqlCommandException(ex.Message, ex, CommandText);
            }
        }

        /// <summary>
        /// Executes a Transact-SQL statement against the connection and returns the number of rows affected.
        /// </summary>
        /// <returns>The number of rows affected.</returns>
        public Task<int> ExecuteNonQueryAsync(CancellationToken cancellationToken)
        {
            if (disposed)
                throw new ObjectDisposedException(GetType().Name);

            try
            {
                return command.ExecuteNonQueryAsync(cancellationToken);
            }
            catch (SqlException ex)
            {
                throw new SqlCommandException(ex.Message, ex, CommandText);
            }
        }

        #region IDisposable Members

        bool disposed;

        /// <summary>
        /// Releases all resources used by the <see cref="SqlCommandAdapter"/>.
        /// </summary>
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
                command.Dispose();
            }

            disposed = true;
        }

        #endregion IDisposable Members
    }
}