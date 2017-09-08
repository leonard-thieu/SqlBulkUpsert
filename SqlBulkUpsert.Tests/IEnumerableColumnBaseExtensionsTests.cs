using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    class IEnumerableColumnBaseExtensionsTests
    {
        [TestClass]
        public class ToSelectListStringMethod
        {
            [TestMethod]
            public void ColumnsIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                IEnumerable<ColumnBase> columns = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    IEnumerableColumnBaseExtensions.ToSelectListString(columns);
                });
            }

            [TestMethod]
            public void ReturnsSelectListString()
            {
                // Arrange
                IEnumerable<ColumnBase> columns = new List<ColumnBase>
                {
                    new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
                    new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
                };

                // Act
                var select = IEnumerableColumnBaseExtensions.ToSelectListString(columns);

                // Assert
                Assert.AreEqual("[key_part_1], [key_part_2]", select);
            }
        }

        [TestClass]
        public class ToColumnDefinitionListStringMethod
        {
            [TestMethod]
            public void ColumnsIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                IEnumerable<ColumnBase> columns = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    IEnumerableColumnBaseExtensions.ToColumnDefinitionListString(columns);
                });
            }

            [TestMethod]
            public void ReturnsColumnsDefinitionList()
            {
                // Arrange
                IEnumerable<ColumnBase> columns = new List<ColumnBase>
                {
                    new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
                    new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
                };

                // Act
                var columnDefinition = IEnumerableColumnBaseExtensions.ToColumnDefinitionListString(columns);

                // Assert
                Assert.AreEqual("[key_part_1] nchar(4) NOT NULL, [key_part_2] smallint NOT NULL", columnDefinition);
            }
        }
    }
}
