﻿using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    class TypedDataReaderTests
    {
        [TestClass]
        public class Constructor
        {
            [TestMethod]
            public void ColumnMappingsIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                ColumnMappings<object> columnMappings = null;
                var items = new List<object>();

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    new TypedDataReader<object>(columnMappings, items);
                });
            }

            [TestMethod]
            public void ItemsIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var columnMappings = new ColumnMappings<object>("myTableName");
                IEnumerable<object> items = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    new TypedDataReader<object>(columnMappings, items);
                });
            }


            [TestMethod]
            public void ReturnsInstance()
            {
                // Arrange
                var columnMappings = new ColumnMappings<object>("myTableName");
                var items = new List<object>();

                // Act
                var reader = new TypedDataReader<object>(columnMappings, items);

                // Assert
                Assert.IsInstanceOfType(reader, typeof(TypedDataReader<object>));
            }
        }

        [TestClass]
        public class GetValueMethod
        {
            [TestMethod]
            public void ReturnsValue()
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
                var items = new List<TestDto>
                {
                    new TestDto
                    {
                        KeyPart1 = "TEST",
                        KeyPart2 = 1,
                        Text = "some text here 1",
                        Number = 1,
                        Date = new DateTimeOffset(new DateTime(2010, 11, 14, 12, 0, 0), TimeSpan.FromHours(1)),
                    },
                };
                var reader = new TypedDataReader<TestDto>(columnMappings, items);
                reader.Read();

                // Act
                var value = reader.GetValue(0);

                // Assert
                Assert.AreEqual("TEST", value);
            }
        }

        [TestClass]
        public class GetOrdinalMethod
        {
            [TestMethod]
            public void ReturnsOrdinal()
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
                var items = new List<TestDto>
                {
                    new TestDto
                    {
                        KeyPart1 = "TEST",
                        KeyPart2 = 1,
                        Text = "some text here 1",
                        Number = 1,
                        Date = new DateTimeOffset(new DateTime(2010, 11, 14, 12, 0, 0), TimeSpan.FromHours(1)),
                    },
                };
                var reader = new TypedDataReader<TestDto>(columnMappings, items);

                // Act
                var ordinal = reader.GetOrdinal("nullable_text");

                // Assert
                Assert.AreEqual(2, ordinal);
            }
        }

        [TestClass]
        public class FieldCountProperty
        {
            [TestMethod]
            public void ReturnsFieldCount()
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
                var items = new List<TestDto>
                {
                    new TestDto
                    {
                        KeyPart1 = "TEST",
                        KeyPart2 = 1,
                        Text = "some text here 1",
                        Number = 1,
                        Date = new DateTimeOffset(new DateTime(2010, 11, 14, 12, 0, 0), TimeSpan.FromHours(1)),
                    },
                };
                var reader = new TypedDataReader<TestDto>(columnMappings, items);

                // Act
                var fieldCount = reader.FieldCount;

                // Assert
                Assert.AreEqual(5, fieldCount);
            }
        }

        [TestClass]
        public class ReadMethod
        {
            [TestMethod]
            public void ReadIsSuccessful_ReturnsTrue()
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
                var items = new List<TestDto>
                {
                    new TestDto
                    {
                        KeyPart1 = "TEST",
                        KeyPart2 = 1,
                        Text = "some text here 1",
                        Number = 1,
                        Date = new DateTimeOffset(new DateTime(2010, 11, 14, 12, 0, 0), TimeSpan.FromHours(1)),
                    },
                };
                var reader = new TypedDataReader<TestDto>(columnMappings, items);

                // Act
                var isSuccessful = reader.Read();

                // Assert
                Assert.IsTrue(isSuccessful);
            }

            [TestMethod]
            public void ReadIsNotSuccessful_ReturnsFalse()
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
                var items = new List<TestDto>
                {
                    new TestDto
                    {
                        KeyPart1 = "TEST",
                        KeyPart2 = 1,
                        Text = "some text here 1",
                        Number = 1,
                        Date = new DateTimeOffset(new DateTime(2010, 11, 14, 12, 0, 0), TimeSpan.FromHours(1)),
                    },
                };
                var reader = new TypedDataReader<TestDto>(columnMappings, items);

                // Act
                reader.Read();
                var isSuccessful = reader.Read();

                // Assert
                Assert.IsFalse(isSuccessful);
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
