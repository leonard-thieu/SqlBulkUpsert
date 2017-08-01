﻿using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Test
{
    [TestClass]
    public class ColumnTests
    {
        readonly Dictionary<Column, string> columnDefn = new Dictionary<Column, string>
        {
            {
                new TextColumn
                {
                    Name = "key_part_1",
                    OrdinalPosition = 1,
                    Nullable = false,
                    DataType = "nchar",
                    CharLength = 4,
                    ByteLength = 8,
                }, "[key_part_1] nchar(4) NOT NULL"
            },
            {
                new NumericColumn
                {
                    Name = "key_part_2",
                    OrdinalPosition = 2,
                    Nullable = false,
                    DataType = "smallint",
                    Precision = 5,
                    Radix = 10,
                    Scale = 0,
                }, "[key_part_2] smallint NOT NULL"
            },
            {
                new TextColumn
                {
                    Name = "nullable_text",
                    OrdinalPosition = 3,
                    Nullable = true,
                    DataType = "nvarchar",
                    CharLength = 50,
                    ByteLength = 100,
                }, "[nullable_text] nvarchar(50) NULL"
            },
            {
                new NumericColumn
                {
                    Name = "nullable_number",
                    OrdinalPosition = 4,
                    Nullable = true,
                    DataType = "int",
                    Precision = 10,
                    Radix = 10,
                    Scale = 0,
                }, "[nullable_number] int NULL"
            },
            {
                new DateColumn
                {
                    Name = "nullable_datetimeoffset",
                    OrdinalPosition = 5,
                    Nullable = true,
                    DataType = "datetimeoffset",
                    Precision = 7,
                }, "[nullable_datetimeoffset] datetimeoffset(7) NULL"
            },
            {
                new NumericColumn
                {
                    Name = "nullable_money",
                    OrdinalPosition = 6,
                    Nullable = true,
                    DataType = "money",
                    Precision = 19,
                    Radix = 10,
                    Scale = 4,
                }, "[nullable_money] money NULL"
            },
            {
                new TextColumn
                {
                    Name = "nullable_varbinary",
                    OrdinalPosition = 7,
                    Nullable = true,
                    DataType = "varbinary",
                    CharLength = -1,
                    ByteLength = -1,
                }, "[nullable_varbinary] varbinary(max) NULL"
            },
            {
                new TextColumn
                {
                    Name = "nullable_image",
                    OrdinalPosition = 8,
                    Nullable = true,
                    DataType = "image",
                    CharLength = 2147483647,
                    ByteLength = 2147483647,
                }, "[nullable_image] image NULL"
            },
            {
                new Column
                {
                    Name = "nullable_xml",
                    OrdinalPosition = 9,
                    Nullable = true,
                    DataType = "xml",
                }, "[nullable_xml] xml NULL"
            },
        };

        [TestMethod]
        public void CheckGeneratedColumnDefinitionString()
        {
            foreach (var kvp in columnDefn)
            {
                Assert.AreEqual(kvp.Value, kvp.Key.ToColumnDefinitionString());
            }
        }
    }
}
