using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace SqlBulkUpsert
{
    public sealed class SqlConnectionAdapter : ISqlConnection
    {
        /// <summary>
        /// Initializes an instance of the <see cref="SqlConnectionAdapter"/> class.
        /// </summary>
        /// <param name="sqlConnection">The connection to wrap.</param>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="sqlConnection"/> is null.
        /// </exception>
        public SqlConnectionAdapter(SqlConnection sqlConnection)
        {
            this.sqlConnection = sqlConnection ?? throw new ArgumentNullException(nameof(sqlConnection));
        }

        readonly SqlConnection sqlConnection;

        /// <summary>
        /// Gets or sets the string used to open a database.
        /// </summary>
        public string ConnectionString
        {
            get => sqlConnection.ConnectionString;
            set => sqlConnection.ConnectionString = value;
        }

        /// <summary>
        /// Gets the time to wait while trying to establish a connection before terminating
        /// the attempt and generating an error.
        /// </summary>
        public int ConnectionTimeout => sqlConnection.ConnectionTimeout;

        /// <summary>
        /// Gets the name of the current database or the database to be used after a connection
        /// is opened.
        /// </summary>
        public string Database => sqlConnection.Database;

        /// <summary>
        /// Gets the current state of the connection.
        /// </summary>
        public ConnectionState State => sqlConnection.State;

        /// <summary>
        /// Begins a database transaction.
        /// </summary>
        /// <returns>An object representing the new transaction.</returns>
        public IDbTransaction BeginTransaction() => sqlConnection.BeginTransaction();

        /// <summary>
        /// Begins a database transaction with the specified <see cref="IsolationLevel"/> value.
        /// </summary>
        /// <param name="il">One of the <see cref="IsolationLevel"/> values.</param>
        /// <returns>An object representing the new transaction.</returns>
        public IDbTransaction BeginTransaction(IsolationLevel il) => sqlConnection.BeginTransaction(il);

        /// <summary>
        /// Changes the current database for an open <see cref="SqlConnection"/>.
        /// </summary>
        /// <param name="databaseName">The name of the database to use instead of the current database.</param>
        /// <exception cref="ArgumentException">
        /// The database name is not valid.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// The connection is not open.
        /// </exception>
        /// <exception cref="SqlException">
        /// Cannot change the database.
        /// </exception>
        public void ChangeDatabase(string databaseName) => sqlConnection.ChangeDatabase(databaseName);

        /// <summary>
        /// Closes the connection to the database.
        /// </summary>
        public void Close() => sqlConnection.Close();

        /// <summary>
        /// Creates and returns a <see cref="SqlCommand"/> object associated with
        /// the <see cref="ISqlConnection"/>.
        /// </summary>
        /// <returns>A <see cref="SqlCommand"/> object.</returns>
        public SqlCommand CreateCommand() => sqlConnection.CreateCommand();

        /// <summary>
        /// Creates and returns a Command object associated with the connection.
        /// </summary>
        /// <returns>A Command object associated with the connection.</returns>
        IDbCommand IDbConnection.CreateCommand() => ((IDbConnection)sqlConnection).CreateCommand();

        /// <summary>
        /// Opens a database connection with the settings specified by the ConnectionString
        /// property of the provider-specific Connection object.
        /// </summary>
        public void Open() => sqlConnection.Open();

        /// <summary>
        /// Opens a database connection with the property settings specified by the <see cref="ConnectionString"/>.
        /// The cancellation token can be used to request that the operation be abandoned
        /// before the connection timeout elapses. Exceptions will be propagated via the
        /// returned Task. If the connection timeout time elapses without successfully connecting,
        /// the returned Task will be marked as faulted with an Exception. The implementation
        /// returns a Task without blocking the calling thread for both pooled and non-pooled
        /// connections.
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="InvalidOperationException">
        /// Calling <see cref="OpenAsync(CancellationToken)"/> more than once for the same instance before task completion.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// Context Connection=true is specified in the connection string.
        /// </exception>
        /// <exception cref="InvalidOperationException">
        /// A connection was not available from the connection pool before the connection time out elapsed.
        /// </exception>
        public Task OpenAsync(CancellationToken cancellationToken) => sqlConnection.OpenAsync(cancellationToken);

        /// <summary>
        /// Releases all resources used by the <see cref="SqlConnectionAdapter"/>.
        /// </summary>
        void IDisposable.Dispose() => sqlConnection.Dispose();
    }
}
