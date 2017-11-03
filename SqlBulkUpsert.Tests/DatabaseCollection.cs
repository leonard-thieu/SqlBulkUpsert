using Xunit;

namespace SqlBulkUpsert.Tests
{
    [CollectionDefinition(Name)]
    public sealed class DatabaseCollection : ICollectionFixture<DatabaseFixture>
    {
        public const string Name = "Database collection";
    }
}
