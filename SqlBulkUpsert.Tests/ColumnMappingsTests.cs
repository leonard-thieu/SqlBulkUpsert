using System;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    class ColumnMappingsTests
    {
        [TestClass]
        public class Constructor
        {
            [TestMethod]
            public void TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                string tableName = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    new ColumnMappings<object>(tableName);
                });
            }

            [TestMethod]
            public void ReturnsInstance()
            {
                // Arrange -> Act
                var mapping = new ColumnMappings<object>("myTableName");

                // Assert
                Assert.IsInstanceOfType(mapping, typeof(ColumnMappings<object>));
            }
        }

        [TestClass]
        public class TableNameProperty
        {
            [TestMethod]
            public void ReturnsTableName()
            {
                // Arrange
                var mappings = new ColumnMappings<object>("myTableName");

                // Act
                var tableName = mappings.TableName;

                // Assert
                Assert.AreEqual("myTableName", tableName);
            }
        }

        [TestClass]
        public class ColumnsProperty
        {
            [TestMethod]
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
                CollectionAssert.AllItemsAreInstancesOfType(columns.ToList(), typeof(string));
            }
        }

        [TestClass]
        public class Add
        {
            [TestMethod]
            public void MappingIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var mapping = new ColumnMappings<object>("tableName");
                Expression<Func<object, object>> map = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    mapping.Add(map);
                });
            }

            [TestMethod]
            public void MappingReferencesValueType_AddsMapping()
            {
                // Arrange
                var mappings = new ColumnMappings<TestDto>("myTableName");

                // Act
                mappings.Add(t => t.KeyPart2);

                // Assert
                var mapping = mappings[0];
                Assert.AreEqual(nameof(TestDto.KeyPart2), mapping.Key);
                var testDto = new TestDto { KeyPart2 = 16 };
                Assert.AreEqual((short)16, mapping.Value(testDto));
            }

            [TestMethod]
            public void MappingReferencesReferenceType_AddsMapping()
            {
                // Arrange
                var mappings = new ColumnMappings<TestDto>("myTableName");

                // Act
                mappings.Add(t => t.KeyPart1);

                // Assert
                var mapping = mappings[0];
                Assert.AreEqual(nameof(TestDto.KeyPart1), mapping.Key);
                var testDto = new TestDto { KeyPart1 = "myKey" };
                Assert.AreEqual("myKey", mapping.Value(testDto));
            }
        }

        class TestDto
        {
            public string KeyPart1 { get; set; }
            public short KeyPart2 { get; set; }
            public string Text { get; set; }
            public int Number { get; set; }
            public DateTimeOffset Date { get; set; }
        }
    }
}
