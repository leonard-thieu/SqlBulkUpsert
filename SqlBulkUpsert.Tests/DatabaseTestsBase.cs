using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    [TestCategory("Integration")]
    [TestCategory("Database")]
    public abstract class DatabaseTestsBase
    {
        [TestInitialize]
        public void TestInitialize()
        {
            DatabaseHelper.CreateDatabase();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            DatabaseHelper.DropDatabase();
        }
    }
}