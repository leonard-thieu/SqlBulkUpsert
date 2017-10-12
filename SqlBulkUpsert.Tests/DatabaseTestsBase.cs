using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    [TestCategory("Uses SQL Server")]
    public abstract class DatabaseTestsBase
    {
        [TestInitialize]
        public async Task TestInitialize()
        {
            await DatabaseHelper.CreateDatabaseAsync().ConfigureAwait(false);
        }

        [TestCleanup]
        public async Task TestCleanup()
        {
            await DatabaseHelper.DropDatabaseAsync().ConfigureAwait(false);
        }
    }
}