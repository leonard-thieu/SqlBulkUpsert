using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;

namespace SqlBulkUpsert
{
    public interface ITypedUpserter<T>
    {
        Task<int> InsertAsync(
            SqlConnection connection,
            IEnumerable<T> items,
            CancellationToken cancellationToken = default(CancellationToken));
        Task<int> UpsertAsync(
            SqlConnection connection,
            IEnumerable<T> items,
            bool updateOnMatch,
            CancellationToken cancellationToken = default(CancellationToken));
    }
}