using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    class TypedUpserterTests
    {
        [TestClass]
        public class Constructor
        {
            [TestMethod]
            public void ColumnMappingsIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                ColumnMappings<object> columnMappings = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    new TypedUpserter<object>(columnMappings);
                });
            }

            [TestMethod]
            public void ReturnsInstance()
            {
                // Arrange
                var columnMappings = new ColumnMappings<object>("myTableName");

                // Act
                var upserter = new TypedUpserter<object>(columnMappings);

                // Assert
                Assert.IsInstanceOfType(upserter, typeof(TypedUpserter<object>));
            }
        }

        [TestClass]
        public class InsertAsyncMethod
        {
            [TestMethod]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var columnMappings = new ColumnMappings<object>("myTableName");
                var upserter = new TypedUpserter<object>(columnMappings);
                SqlConnection connection = null;
                var items = new List<object>();

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return upserter.InsertAsync(connection, items);
                });
            }

            [TestMethod]
            public async Task ItemsIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var columnMappings = new ColumnMappings<object>("myTableName");
                var upserter = new TypedUpserter<object>(columnMappings);
                var connection = new SqlConnection();
                IEnumerable<object> items = null;

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return upserter.InsertAsync(connection, items);
                });
            }
        }

        [TestClass]
        public class UpsertAsyncMethod
        {
            [TestMethod]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var columnMappings = new ColumnMappings<object>("myTableName");
                var upserter = new TypedUpserter<object>(columnMappings);
                SqlConnection connection = null;
                var items = new List<object>();
                var updateWhenMatched = false;

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return upserter.UpsertAsync(connection, items, updateWhenMatched);
                });
            }

            [TestMethod]
            public async Task ItemsIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var columnMappings = new ColumnMappings<object>("myTableName");
                var upserter = new TypedUpserter<object>(columnMappings);
                var connection = new SqlConnection();
                IEnumerable<object> items = null;
                var updateWhenMatched = false;

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return upserter.UpsertAsync(connection, items, updateWhenMatched);
                });
            }
        }

        [TestClass]
        public class EndToEnd : DatabaseTestsBase
        {
            [TestMethod]
            public async Task EndToEndTest()
            {
                // Arrange
                var columnMappings = new ColumnMappings<TestDto>(Constants.TableName)
                {
                    { "key_part_1", d => d.KeyPart1 },
                    { "key_part_2", d => d.KeyPart2 },
                    { "nullable_text", d => d.Text },
                    { "nullable_number", d => d.Number },
                    { "nullable_datetimeoffset", d => d.Date },
                };

                var upserter = new TypedUpserter<TestDto>(columnMappings);

                var items = new List<TestDto>();

                for (int i = 1; i <= 10; i++)
                {
                    items.Add(new TestDto
                    {
                        KeyPart1 = "TEST",
                        KeyPart2 = (short)i,
                        Text = $"some text here {i}",
                        Number = i,
                        Date = new DateTimeOffset(new DateTime(2010, 11, 14, 12, 0, 0), TimeSpan.FromHours(i)),
                    });
                }

                using (var connection = DatabaseHelper.CreateAndOpenConnection(Constants.DatabaseName))
                {
                    // Act
                    await upserter.UpsertAsync(
                        connection,
                        items,
                        updateWhenMatched: false);

                    // Assert
                    foreach (var testDto in items)
                    {
                        Assert.AreEqual(testDto.Number, testDto.KeyPart2);
                    }
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
}