using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    [TestCategory("Uses SQL Server")]
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