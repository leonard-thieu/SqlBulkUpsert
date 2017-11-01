using System.Collections.Generic;
using Xunit;

namespace SqlBulkUpsert.Tests
{
    public class ColumnTests
    {
        private readonly Dictionary<ColumnBase, string> columnDefn = new Dictionary<ColumnBase, string>
        {
            {
                new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
                "[key_part_1] nchar(4) NOT NULL"
            },
            {
                new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
                "[key_part_2] smallint NOT NULL"
            },
            {
                new StringColumn("nullable_text", 3, true, "nvarchar", 50, 100),
                "[nullable_text] nvarchar(50) NULL"
            },
            {
                new NumericColumn("nullable_number", 4, true, "int", 10, 10, 0),
                "[nullable_number] int NULL"
            },
            {
                new DateTimeColumn("nullable_datetimeoffset", 5, true, "datetimeoffset", 7),
                "[nullable_datetimeoffset] datetimeoffset(7) NULL"
            },
            {
                new NumericColumn("nullable_money", 6, true, "money", 19, 10, 4),
                "[nullable_money] money NULL"
            },
            {
                new StringColumn("nullable_varbinary", 7, true, "varbinary", -1, -1),
                "[nullable_varbinary] varbinary(max) NULL"
            },
            {
                new StringColumn("nullable_image", 8, true, "image", 2147483647, 2147483647),
                "[nullable_image] image NULL"
            },
        };

        [Fact]
        public void CheckGeneratedColumnDefinitionString()
        {
            foreach (var kvp in columnDefn)
            {
                Assert.Equal(kvp.Value, kvp.Key.ToColumnDefinitionString());
            }
        }
    }
}
