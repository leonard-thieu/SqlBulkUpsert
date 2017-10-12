using System;
using System.Collections;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    class ColumnsTests
    {
        [TestClass]
        public class Constructor
        {
            [TestMethod]
            public void ColumnsIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                IEnumerable<ColumnBase> cols = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    new Columns(cols);
                });
            }

            [TestMethod]
            public void ReturnsInstance()
            {
                // Arrange
                var cols = Helper.FakeColumns;

                // Act
                var columns = new Columns(cols);

                // Assert
                Assert.IsInstanceOfType(columns, typeof(Columns));
            }
        }

        [TestClass]
        public class ToSelectListStringMethod
        {
            [TestMethod]
            public void ReturnsSelectListString()
            {
                // Arrange
                var columns = new Columns(new List<ColumnBase>
                {
                    new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
                    new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
                });

                // Act
                var select = columns.ToSelectListString();

                // Assert
                Assert.AreEqual("[key_part_1], [key_part_2]", select);
            }
        }

        [TestClass]
        public class ToColumnDefinitionListStringMethod
        {

            [TestMethod]
            public void ReturnsColumnsDefinitionList()
            {
                // Arrange
                var columns = new Columns(new List<ColumnBase>
                {
                    new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
                    new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
                });

                // Act
                var columnDefinition = columns.ToColumnDefinitionListString();

                // Assert
                Assert.AreEqual("[key_part_1] nchar(4) NOT NULL, [key_part_2] smallint NOT NULL", columnDefinition);
            }
        }

        [TestClass]
        public class GetEnumeratorMethod
        {
            [TestMethod]
            public void ReturnsEnumerator()
            {
                // Arrange
                var cols = Helper.FakeColumns;
                var columns = new Columns(cols);

                // Act
                var enumerator = columns.GetEnumerator();

                // Assert
                Assert.IsInstanceOfType(enumerator, typeof(IEnumerator<ColumnBase>));
            }
        }

        [TestClass]
        public class IEnumerable_GetEnumeratorMethod
        {
            [TestMethod]
            public void ReturnsEnumerator()
            {
                // Arrange
                var cols = Helper.FakeColumns;
                var columns = new Columns(cols);
                var enumerable = (IEnumerable)columns;

                // Act
                var enumerator = enumerable.GetEnumerator();

                // Assert
                Assert.IsInstanceOfType(enumerator, typeof(IEnumerator));
            }
        }
    }
}
