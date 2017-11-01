using System.Threading.Tasks;
using Xunit;

namespace SqlBulkUpsert.Tests
{
    [Trait("Category", "Uses SQL Server")]
    public abstract class DatabaseTestsBase : IAsyncLifetime
    {
        public async Task InitializeAsync()
        {
            await DatabaseHelper.CreateDatabaseAsync().ConfigureAwait(false);
        }

        public async Task DisposeAsync()
        {
            await DatabaseHelper.DropDatabaseAsync().ConfigureAwait(false);
        }
    }
}