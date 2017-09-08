using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    [TestClass]
    public class SqlTableSchemaTests : DatabaseTestsBase
    {
        [TestMethod]
        public async Task RetrieveTableSchemaNotExist()
        {
            // Arrange
            using (var connection = DatabaseHelper.CreateAndOpenConnection())
            {
                // Act -> Assert
                await Assert.ThrowsExceptionAsync<InvalidOperationException>(() =>
                {
                    return SqlTableSchema.LoadFromDatabaseAsync(
                        connection,
                        "DoesNotExist",
                        CancellationToken.None);
                });
            }
        }

        [TestMethod]
        public async Task PopulateFromReader()
        {
            // Arrange
            var columnDetail = new DataTable();

            columnDetail.Columns.Add("COLUMN_NAME", typeof(string));
            columnDetail.Columns.Add("ORDINAL_POSITION", typeof(int));
            columnDetail.Columns.Add("IS_NULLABLE", typeof(string));
            columnDetail.Columns.Add("DATA_TYPE", typeof(string));
            columnDetail.Columns.Add("CHARACTER_MAXIMUM_LENGTH", typeof(int)).AllowDBNull = true;
            columnDetail.Columns.Add("CHARACTER_OCTET_LENGTH", typeof(int)).AllowDBNull = true;
            columnDetail.Columns.Add("NUMERIC_PRECISION", typeof(byte)).AllowDBNull = true;
            columnDetail.Columns.Add("NUMERIC_PRECISION_RADIX", typeof(short)).AllowDBNull = true;
            columnDetail.Columns.Add("NUMERIC_SCALE", typeof(int)).AllowDBNull = true;
            columnDetail.Columns.Add("DATETIME_PRECISION", typeof(short)).AllowDBNull = true;

            columnDetail.Rows.Add("key_part_1", 1, "NO", "nchar", 4, 8, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);
            columnDetail.Rows.Add("key_part_2", 2, "NO", "smallint", DBNull.Value, DBNull.Value, (byte)5, (short)10, 0, DBNull.Value);
            columnDetail.Rows.Add("nullable_text", 3, "YES", "nvarchar", 50, 100, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);
            columnDetail.Rows.Add("nullable_number", 4, "YES", "int", DBNull.Value, DBNull.Value, (byte)10, (short)10, 0, DBNull.Value);
            columnDetail.Rows.Add("nullable_datetimeoffset", 5, "YES", "datetimeoffset", DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, (short)7);
            columnDetail.Rows.Add("nullable_money", 6, "YES", "money", DBNull.Value, DBNull.Value, (byte)19, (short)10, 4, DBNull.Value);
            columnDetail.Rows.Add("nullable_varbinary", 7, "YES", "varbinary", -1, -1, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);
            columnDetail.Rows.Add("nullable_image", 8, "YES", "image", 2147483647, 2147483647, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);

            var keyDetail = new DataTable();

            keyDetail.Columns.Add("COLUMN_NAME", typeof(string));

            keyDetail.Rows.Add("key_part_1");
            keyDetail.Rows.Add("key_part_2");

            var dataTableReader = new DataTableReader(new[] { columnDetail, keyDetail });

            var expectedColumns = new List<Column>
            {
                new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
                new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
                new StringColumn("nullable_text", 3, true, "nvarchar", 50, 100),
                new NumericColumn("nullable_number", 4, true, "int", 10, 10, 0),
                new DateTimeColumn("nullable_datetimeoffset", 5, true, "datetimeoffset", 7),
                new NumericColumn("nullable_money", 6, true, "money", 19, 10, 4),
                new StringColumn("nullable_varbinary", 7, true, "varbinary", -1, -1),
                new StringColumn("nullable_image", 8, true, "image", 2147483647, 2147483647),
            };

            var expectedKeyColumns = new List<Column>
            {
                new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
                new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
            };

            // Act
            var schema = await SqlTableSchema.LoadFromReaderAsync(
                Constants.TableName,
                dataTableReader,
                CancellationToken.None);

            // Assert 
            Assert.AreEqual(Constants.TableName, schema.TableName);
            CollectionAssert.AreEqual(expectedColumns, schema.Columns as ICollection, new ColumnComparer());
            CollectionAssert.AreEqual(expectedKeyColumns, schema.PrimaryKeyColumns as ICollection, new ColumnComparer());
        }

        [TestMethod]
        public async Task RetrieveTableSchema()
        {
            // Arrange
            using (var connection = DatabaseHelper.CreateAndOpenConnection(Constants.DatabaseName))
            {
                var expectedColumns = new List<Column>
                {
                    new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
                    new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
                    new StringColumn("nullable_text", 3, true, "nvarchar", 50, 100),
                    new NumericColumn("nullable_number", 4, true, "int", 10, 10, 0),
                    new DateTimeColumn("nullable_datetimeoffset", 5, true, "datetimeoffset", 7),
                    new NumericColumn("nullable_money", 6, true, "money", 19, 10, 4),
                    new StringColumn("nullable_varbinary", 7, true, "varbinary", -1, -1),
                    new StringColumn("nullable_image", 8, true, "image", 2147483647, 2147483647),
                };

                var expectedKeyColumns = new List<Column>
                {
                    new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
                    new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
                };

                // Act
                SqlTableSchema schema = await SqlTableSchema.LoadFromDatabaseAsync(
                    connection,
                    Constants.TableName,
                    CancellationToken.None);

                // Assert
                Assert.AreEqual(Constants.TableName, schema.TableName);
                CollectionAssert.AreEqual(expectedColumns, schema.Columns as ICollection, new ColumnComparer());
                CollectionAssert.AreEqual(expectedKeyColumns, schema.PrimaryKeyColumns as ICollection, new ColumnComparer());
            }
        }

        [TestMethod]
        public void CheckCreateTableCommand()
        {
            // Arrange
            var schema = new SqlTableSchema(
                Constants.TableName,
                new List<Column>
                {
                    new NumericColumn("first", 1, false, "int"),
                    new StringColumn("second", 2, true, "ntext"),
                    new DateTimeColumn("third", 3, false, "datetime2", 4),
                },
                new List<string>());

            // Act
            var cmdText = schema.ToCreateTableCommandText();

            // Assert
            Assert.AreEqual($"CREATE TABLE [{Constants.TableName}] ([first] int NOT NULL, [second] ntext NULL, [third] datetime2(4) NOT NULL);", cmdText);
        }

        [TestMethod]
        public void CheckDropTableCommand()
        {
            // Arrange
            var schema = new SqlTableSchema(
                Constants.TableName,
                new List<Column>
                {
                    new NumericColumn("first", 1, false, "int"),
                    new StringColumn("second", 2, true, "ntext"),
                    new DateTimeColumn("third", 3, false, "datetime2", 4),
                },
                new List<string>());

            // Act
            var cmdText = schema.ToDropTableCommandText();

            // Assert
            Assert.AreEqual($"DROP TABLE [{Constants.TableName}];", cmdText);
        }
    }
}
