using System.Collections.Generic;

namespace SqlBulkUpsert.Tests
{
    internal static class Helper
    {
        public static IEnumerable<ColumnBase> FakeColumns { get; } = new List<ColumnBase>
        {
            new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
            new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
            new StringColumn("nullable_text", 3, true, "nvarchar", 50, 100),
            new NumericColumn("nullable_number", 4, true, "int", 10, 10, 0),
            new DateTimeColumn("nullable_datetimeoffset", 5, true, "datetimeoffset", 7),
            new NumericColumn("nullable_money", 6, true, "money", 19, 10, 4),
            new StringColumn("nullable_varbinary", 7, true, "varbinary", -1, -1),
            new StringColumn("nullable_image", 8, true, "image", 2147483647, 2147483647),
        }.AsReadOnly();

        public static IEnumerable<string> FakePrimaryKeyColumnNames { get; } = new List<string> { "key_part_1", "key_part_2" }.AsReadOnly();

        public static SqlTableSchema FakeSqlTableSchema { get; } = new SqlTableSchema("myTableName", FakeColumns, FakePrimaryKeyColumnNames);
    }
}
