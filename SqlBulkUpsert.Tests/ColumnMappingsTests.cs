using System;
using System.Linq;
using System.Linq.Expressions;
using Xunit;

namespace SqlBulkUpsert.Tests
{
    public class ColumnMappingsTests
    {
        public class Constructor
        {
            [Fact]
            public void TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                string tableName = null;

                // Act -> Assert
                Assert.Throws<ArgumentNullException>(() =>
                {
                    new ColumnMappings<object>(tableName);
                });
            }

            [Fact]
            public void ReturnsInstance()
            {
                // Arrange -> Act
                var mapping = new ColumnMappings<object>("myTableName");

                // Assert
                Assert.IsAssignableFrom<ColumnMappings<object>>(mapping);
            }
        }

        public class TableNameProperty
        {
            [Fact]
            public void ReturnsTableName()
            {
                // Arrange
                var mappings = new ColumnMappings<object>("myTableName");

                // Act
                var tableName = mappings.TableName;

                // Assert
                Assert.Equal("myTableName", tableName);
            }
        }

        public class ColumnsProperty
        {
            [Fact]
            public void ReturnsColumns()
            {
                // Arrange
                var columnMappings = new ColumnMappings<TestDto>("myTableName")
                {
                    { "key_part_1", d => d.KeyPart1 },
                    { "key_part_2", d => d.KeyPart2 },
                    { "nullable_text", d => d.Text },
                    { "nullable_number", d => d.Number },
                    { "nullable_datetimeoffset", d => d.Date },
                };

                // Act
                var columns = columnMappings.Columns;

                // Assert
                Assert.All(columns.ToList(), c => Assert.IsAssignableFrom<string>(c));
            }
        }

        public class Add
        {
            [Fact]
            public void MappingIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var mapping = new ColumnMappings<object>("tableName");
                Expression<Func<object, object>> map = null;

                // Act -> Assert
                Assert.Throws<ArgumentNullException>(() =>
                {
                    mapping.Add(map);
                });
            }

            [Fact]
            public void MappingReferencesValueType_AddsMapping()
            {
                // Arrange
                var mappings = new ColumnMappings<TestDto>("myTableName");

                // Act
                mappings.Add(t => t.KeyPart2);

                // Assert
                var mapping = mappings[0];
                Assert.Equal(nameof(TestDto.KeyPart2), mapping.Key);
                var testDto = new TestDto { KeyPart2 = 16 };
                Assert.Equal((short)16, mapping.Value(testDto));
            }

            [Fact]
            public void MappingReferencesReferenceType_AddsMapping()
            {
                // Arrange
                var mappings = new ColumnMappings<TestDto>("myTableName");

                // Act
                mappings.Add(t => t.KeyPart1);

                // Assert
                var mapping = mappings[0];
                Assert.Equal(nameof(TestDto.KeyPart1), mapping.Key);
                var testDto = new TestDto { KeyPart1 = "myKey" };
                Assert.Equal("myKey", mapping.Value(testDto));
            }
        }

        private class TestDto
        {
            public string KeyPart1 { get; set; }
            public short KeyPart2 { get; set; }
            public string Text { get; set; }
            public int Number { get; set; }
            public DateTimeOffset Date { get; set; }
        }
    }
}
