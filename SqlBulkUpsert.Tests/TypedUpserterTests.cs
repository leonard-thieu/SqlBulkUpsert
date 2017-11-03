using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Xunit;

namespace SqlBulkUpsert.Tests
{
    public class TypedUpserterTests
    {
        public class Constructor
        {
            [Fact]
            public void ColumnMappingsIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                ColumnMappings<object> columnMappings = null;

                // Act -> Assert
                Assert.Throws<ArgumentNullException>(() =>
                {
                    new TypedUpserter<object>(columnMappings);
                });
            }

            [Fact]
            public void ReturnsInstance()
            {
                // Arrange
                var columnMappings = new ColumnMappings<object>("myTableName");

                // Act
                var upserter = new TypedUpserter<object>(columnMappings);

                // Assert
                Assert.IsAssignableFrom<TypedUpserter<object>>(upserter);
            }
        }

        public class InsertAsyncMethod
        {
            [Fact]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var columnMappings = new ColumnMappings<object>("myTableName");
                var upserter = new TypedUpserter<object>(columnMappings);
                SqlConnection connection = null;
                var items = new List<object>();

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return upserter.InsertAsync(connection, items);
                });
            }

            [Fact]
            public async Task ItemsIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var columnMappings = new ColumnMappings<object>("myTableName");
                var upserter = new TypedUpserter<object>(columnMappings);
                var connection = new SqlConnection();
                IEnumerable<object> items = null;

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return upserter.InsertAsync(connection, items);
                });
            }
        }

        public class UpsertAsyncMethod
        {
            [Fact]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var columnMappings = new ColumnMappings<object>("myTableName");
                var upserter = new TypedUpserter<object>(columnMappings);
                SqlConnection connection = null;
                var items = new List<object>();
                var updateWhenMatched = false;

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return upserter.UpsertAsync(connection, items, updateWhenMatched);
                });
            }

            [Fact]
            public async Task ItemsIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var columnMappings = new ColumnMappings<object>("myTableName");
                var upserter = new TypedUpserter<object>(columnMappings);
                var connection = new SqlConnection();
                IEnumerable<object> items = null;
                var updateWhenMatched = false;

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return upserter.UpsertAsync(connection, items, updateWhenMatched);
                });
            }
        }

        [Trait("Category", "Uses SQL Server")]
        [Collection(DatabaseCollection.Name)]
        public class IntegrationTests
        {
            public IntegrationTests(DatabaseFixture fixture)
            {
                this.fixture = fixture;
            }

            private readonly DatabaseFixture fixture;

            [Fact]
            public async Task EndToEnd()
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
                items.Add(new TestDto
                {
                    KeyPart1 = "TEST",
                    KeyPart2 = 11,
                    Text = null,
                    Number = null,
                    Date = null,
                });

                using (var connection = await DatabaseHelper.CreateAndOpenConnectionAsync(Constants.DatabaseName))
                {
                    // Act
                    await upserter.UpsertAsync(connection, items, updateWhenMatched: false);

                    // Assert
                    var items2 = new List<TestDto>();
                    using (var command = connection.CreateCommand())
                    {
                        command.CommandText = @"SELECT [key_part_1]
      ,[key_part_2]
      ,[nullable_text]
      ,[nullable_number]
      ,[nullable_datetimeoffset]
      ,[nullable_money]
      ,[nullable_varbinary]
      ,[nullable_image]
  FROM [SqlBulkUpsertTestDb].[dbo].[TestUpsert]";

                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (reader.Read())
                            {
                                var item = new TestDto();

                                item.KeyPart1 = reader.GetString(0);
                                item.KeyPart2 = reader.GetInt16(1);
                                item.Text = reader.IsDBNull(2) ? null : reader.GetString(2);
                                item.Number = reader.IsDBNull(3) ? (int?)null : reader.GetInt32(3);
                                item.Date = reader.IsDBNull(4) ? (DateTimeOffset?)null : reader.GetDateTimeOffset(4);

                                items2.Add(item);
                            }
                        }
                    }

                    for (int i = 0; i < items.Count; i++)
                    {
                        var item1 = items[i];
                        var item2 = items2[i];

                        Assert.Equal(item1.KeyPart1, item2.KeyPart1);
                        Assert.Equal(item1.KeyPart2, item2.KeyPart2);
                        Assert.Equal(item1.Text, item2.Text);
                        Assert.Equal(item1.Number, item2.Number);
                        Assert.Equal(item1.Date, item2.Date);
                    }
                }
            }

            private class TestDto
            {
                public string KeyPart1 { get; set; }
                public short KeyPart2 { get; set; }
                public string Text { get; set; }
                public int? Number { get; set; }
                public DateTimeOffset? Date { get; set; }
            }
        }
    }
}