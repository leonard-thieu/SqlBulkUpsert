using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace SqlBulkUpsert
{
    public sealed class SqlBulkCopyAdapter : ISqlBulkCopy
    {
        /// <summary>
        /// Initializes an instance of the <see cref="SqlBulkCopyAdapter"/> class.
        /// </summary>
        /// <exception cref="ArgumentNullException">
        /// <paramref name="connection"/> is null.
        /// </exception>
        public SqlBulkCopyAdapter(SqlConnection connection)
        {
            if (connection == null)
                throw new ArgumentNullException(nameof(connection));

            sqlBulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, null);
            Connection = new SqlConnectionAdapter(connection);
        }

        readonly SqlBulkCopy sqlBulkCopy;

        public ISqlConnection Connection { get; }

        /// <summary>
        /// Name of the destination table on the server.
        /// </summary>
        public string DestinationTableName
        {
            get => sqlBulkCopy.DestinationTableName;
            set => sqlBulkCopy.DestinationTableName = value;
        }

        /// <summary>
        /// Returns a collection of <see cref="SqlBulkCopyColumnMapping"/> items.
        /// Column mappings define the relationships between columns in the data source and
        /// columns in the destination.
        /// </summary>
        public SqlBulkCopyColumnMappingCollection ColumnMappings => sqlBulkCopy.ColumnMappings;

        /// <summary>
        /// Number of seconds for the operation to complete before it times out.
        /// </summary>
        public int BulkCopyTimeout
        {
            get => sqlBulkCopy.BulkCopyTimeout;
            set => sqlBulkCopy.BulkCopyTimeout = value;
        }

        /// <summary>
        /// Copies all rows in the supplied <see cref="IDataReader"/> to a destination
        /// table specified by the <see cref="DestinationTableName"/>
        /// property of the <see cref="ISqlBulkCopy"/> object.The cancellation token
        /// can be used to request that the operation be abandoned before the command timeout
        /// elapses. Exceptions will be reported via the returned Task object.
        /// </summary>
        /// <param name="reader">
        /// A <see cref="IDataReader"/> whose rows will be copied to the destination table.
        /// </param>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="System.InvalidOperationException">
        /// Calling <see cref="WriteToServerAsync(IDataReader, CancellationToken)"/>
        /// multiple times for the same instance before task completion.
        /// </exception>
        /// <exception cref="System.InvalidOperationException">
        /// The connection drops or is closed during <see cref="WriteToServerAsync(IDataReader, CancellationToken)"/> 
        /// execution.
        /// </exception>
        /// <exception cref="System.InvalidOperationException">
        /// Returned in the task object, the <see cref="ISqlBulkCopy"/> object was closed during 
        /// the method execution.
        /// </exception>
        /// <exception cref="System.InvalidOperationException">
        /// Returned in the task object, there was a connection pool timeout.
        /// </exception>
        /// <exception cref="System.InvalidOperationException">
        /// Returned in the task object, the <see cref="ISqlConnection"/> object is closed before 
        /// method execution.
        /// </exception>
        /// <exception cref="System.InvalidOperationException">
        /// The <see cref="IDataReader"/> was closed before the completed <see cref="Task"/> returned.
        /// </exception>
        /// <exception cref="System.InvalidOperationException">
        /// The <see cref="IDataReader"/>'s associated connection was closed before the completed 
        /// <see cref="Task"/> returned.
        /// </exception>
        /// <exception cref="System.InvalidOperationException">
        /// Context Connection=true is specified in the connection string.
        /// </exception>
        /// <exception cref="SqlException">
        /// Returned in the task object, any error returned by SQL Server that occurred while
        /// opening the connection.
        /// </exception>
        public Task WriteToServerAsync(IDataReader reader, CancellationToken cancellationToken) =>
            sqlBulkCopy.WriteToServerAsync(reader, cancellationToken);
    }
}
