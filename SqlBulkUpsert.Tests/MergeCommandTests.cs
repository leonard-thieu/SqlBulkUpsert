using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using toofz.TestsShared;

namespace SqlBulkUpsert.Tests
{
    class MergeCommandTests
    {
        [TestClass]
        public class Constructor
        {
            [TestMethod]
            public void TableSourceIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                string tableSource = null;
                var targetTableSchema = new SqlTableSchema("myTableName", new ColumnBase[1], new string[0]);
                var updateWhenMatched = false;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    new MergeCommand(tableSource, targetTableSchema, updateWhenMatched);
                });
            }

            [TestMethod]
            public void TargetTableSchemaIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var tableSource = "myTableSource";
                SqlTableSchema targetTableSchema = null;
                var updateWhenMatched = false;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    new MergeCommand(tableSource, targetTableSchema, updateWhenMatched);
                });
            }

            [TestMethod]
            public void ReturnsInstance()
            {
                // Arrange
                var tableSource = "myTableSource";
                var targetTableSchema = new SqlTableSchema("myTableName", new ColumnBase[1], new string[0]);
                var updateWhenMatched = false;

                // Act
                var merge = new MergeCommand(tableSource, targetTableSchema, updateWhenMatched);

                // Assert
                Assert.IsInstanceOfType(merge, typeof(MergeCommand));
            }
        }

        [TestClass]
        public class ToStringMethod
        {
            [TestMethod]
            public void UpdateWhenMatchedIsTrue_ReturnsMergeCommandWithUpdateWhenMatched()
            {
                // Arrange
                var tableSource = "myTableSource";
                var columns = new List<ColumnBase>
                {
                    new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
                    new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
                    new StringColumn("nullable_text", 3, true, "nvarchar", 50, 100),
                    new NumericColumn("nullable_number", 4, true, "int", 10, 10, 0),
                    new DateTimeColumn("nullable_datetimeoffset", 5, true, "datetimeoffset", 7),
                    new NumericColumn("nullable_money", 6, true, "money", 19, 10, 4),
                    new StringColumn("nullable_varbinary", 7, true, "varbinary", -1, -1),
                    new StringColumn("nullable_image", 8, true, "image", 2147483647, 2147483647),
                };
                var primaryKeyColumnNames = new[] { "key_part_1", "key_part_2" };
                var targetTableSchema = new SqlTableSchema("myTableName", columns, primaryKeyColumnNames);
                var updateWhenMatched = true;
                var merge = new MergeCommand(tableSource, targetTableSchema, updateWhenMatched);

                // Act
                var command = merge.ToString();

                // Assert
                Assert.That.NormalizedAreEqual(@"MERGE INTO [myTableName] AS [Target]
USING [myTableSource] AS [Source]
    ON ([Target].[key_part_1] = [Source].[key_part_1] AND [Target].[key_part_2] = [Source].[key_part_2])
WHEN MATCHED
    THEN
        UPDATE
        SET [Target].[nullable_text] = [Source].[nullable_text],
            [Target].[nullable_number] = [Source].[nullable_number],
            [Target].[nullable_datetimeoffset] = [Source].[nullable_datetimeoffset],
            [Target].[nullable_money] = [Source].[nullable_money],
            [Target].[nullable_varbinary] = [Source].[nullable_varbinary],
            [Target].[nullable_image] = [Source].[nullable_image]
WHEN NOT MATCHED
    THEN
        INSERT ([key_part_1], [key_part_2], [nullable_text], [nullable_number], [nullable_datetimeoffset], [nullable_money], [nullable_varbinary], [nullable_image])
        VALUES ([key_part_1], [key_part_2], [nullable_text], [nullable_number], [nullable_datetimeoffset], [nullable_money], [nullable_varbinary], [nullable_image]);
", command);
            }

            [TestMethod]
            public void UpdateWhenMatchedIsFalse_ReturnsMergeCommand()
            {
                // Arrange
                var tableSource = "myTableSource";
                var columns = new List<ColumnBase>
                {
                    new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
                    new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
                    new StringColumn("nullable_text", 3, true, "nvarchar", 50, 100),
                    new NumericColumn("nullable_number", 4, true, "int", 10, 10, 0),
                    new DateTimeColumn("nullable_datetimeoffset", 5, true, "datetimeoffset", 7),
                    new NumericColumn("nullable_money", 6, true, "money", 19, 10, 4),
                    new StringColumn("nullable_varbinary", 7, true, "varbinary", -1, -1),
                    new StringColumn("nullable_image", 8, true, "image", 2147483647, 2147483647),
                };
                var primaryKeyColumnNames = new[] { "key_part_1", "key_part_2" };
                var targetTableSchema = new SqlTableSchema("myTableName", columns, primaryKeyColumnNames);
                var updateWhenMatched = false;
                var merge = new MergeCommand(tableSource, targetTableSchema, updateWhenMatched);

                // Act
                var command = merge.ToString();

                // Assert
                Assert.That.NormalizedAreEqual(@"MERGE INTO [myTableName] AS [Target]
USING [myTableSource] AS [Source]
    ON ([Target].[key_part_1] = [Source].[key_part_1] AND [Target].[key_part_2] = [Source].[key_part_2])
WHEN NOT MATCHED
    THEN
        INSERT ([key_part_1], [key_part_2], [nullable_text], [nullable_number], [nullable_datetimeoffset], [nullable_money], [nullable_varbinary], [nullable_image])
        VALUES ([key_part_1], [key_part_2], [nullable_text], [nullable_number], [nullable_datetimeoffset], [nullable_money], [nullable_varbinary], [nullable_image]);
", command);
            }
        }
    }
}
