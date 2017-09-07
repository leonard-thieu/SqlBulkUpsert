using System.Data;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace SqlBulkUpsert
{
    public interface ISqlConnection : IDbConnection
    {
        /// <summary>
        /// Creates and returns a <see cref="SqlCommand"/> object associated with
        /// the <see cref="ISqlConnection"/>.
        /// </summary>
        /// <returns>A <see cref="SqlCommand"/> object.</returns>
        new SqlCommand CreateCommand();
        /// <summary>
        /// Opens a database connection with the property settings specified by the <see cref="ISqlConnection.ConnectionString"/>.
        /// The cancellation token can be used to request that the operation be abandoned
        /// before the connection timeout elapses. Exceptions will be propagated via the
        /// returned Task. If the connection timeout time elapses without successfully connecting,
        /// the returned Task will be marked as faulted with an Exception. The implementation
        /// returns a Task without blocking the calling thread for both pooled and non-pooled
        /// connections.
        /// </summary>
        /// <param name="cancellationToken">The cancellation instruction.</param>
        /// <returns>A task representing the asynchronous operation.</returns>
        /// <exception cref="System.InvalidOperationException">
        /// Calling <see cref="OpenAsync(CancellationToken)"/> more than once for the same instance before task completion.
        /// </exception>
        /// <exception cref="System.InvalidOperationException">
        /// Context Connection=true is specified in the connection string.
        /// </exception>
        /// <exception cref="System.InvalidOperationException">
        /// A connection was not available from the connection pool before the connection time out elapsed.
        /// </exception>
        Task OpenAsync(CancellationToken cancellationToken);
    }
}