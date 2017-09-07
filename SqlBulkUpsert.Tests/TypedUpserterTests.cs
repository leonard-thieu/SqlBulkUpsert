using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    [TestClass]
    public class TypedUpserterTests : DatabaseTestsBase
    {
        [TestMethod]
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

            using (var connection = DatabaseHelper.CreateAndOpenConnection(Constants.DatabaseName))
            {
                // Act
                await upserter.UpsertAsync(
                    connection,
                    items,
                    updateOnMatch: false);

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