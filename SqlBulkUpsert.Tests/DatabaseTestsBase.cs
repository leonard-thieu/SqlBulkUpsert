using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    public abstract class DatabaseTestsBase
    {
        [TestInitialize]
        public void TestInitialize()
        {
            DatabaseHelper.RefreshSchema();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            DatabaseHelper.DropDatabase();
        }
    }
}