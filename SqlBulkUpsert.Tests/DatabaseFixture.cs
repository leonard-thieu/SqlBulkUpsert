using System.Threading.Tasks;
using Xunit;

namespace SqlBulkUpsert.Tests
{
    public sealed class DatabaseFixture : IAsyncLifetime
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
