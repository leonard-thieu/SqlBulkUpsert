using System;
using System.Collections;
using System.Collections.Generic;
using Xunit;

namespace SqlBulkUpsert.Tests
{
    public class ColumnsTests
    {
        public class Constructor
        {
            [Fact]
            public void ColumnsIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                IEnumerable<ColumnBase> cols = null;

                // Act -> Assert
                Assert.Throws<ArgumentNullException>(() =>
                {
                    new Columns(cols);
                });
            }

            [Fact]
            public void ReturnsInstance()
            {
                // Arrange
                var cols = Helper.FakeColumns;

                // Act
                var columns = new Columns(cols);

                // Assert
                Assert.IsAssignableFrom<Columns>(columns);
            }
        }

        public class ToSelectListStringMethod
        {
            [Fact]
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
                Assert.Equal("[key_part_1], [key_part_2]", select);
            }
        }

        public class ToColumnDefinitionListStringMethod
        {

            [Fact]
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
                Assert.Equal("[key_part_1] nchar(4) NOT NULL, [key_part_2] smallint NOT NULL", columnDefinition);
            }
        }

        public class GetEnumeratorMethod
        {
            [Fact]
            public void ReturnsEnumerator()
            {
                // Arrange
                var cols = Helper.FakeColumns;
                var columns = new Columns(cols);

                // Act
                var enumerator = columns.GetEnumerator();

                // Assert
                Assert.IsAssignableFrom<IEnumerator<ColumnBase>>(enumerator);
            }
        }

        public class IEnumerable_GetEnumeratorMethod
        {
            [Fact]
            public void ReturnsEnumerator()
            {
                // Arrange
                var cols = Helper.FakeColumns;
                var columns = new Columns(cols);
                var enumerable = (IEnumerable)columns;

                // Act
                var enumerator = enumerable.GetEnumerator();

                // Assert
                Assert.IsAssignableFrom<IEnumerator>(enumerator);
            }
        }
    }
}
