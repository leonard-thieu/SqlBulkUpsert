using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    class SqlTableSchemaTests
    {
        [TestClass]
        public class Cosntructor
        {
            [TestMethod]
            public void TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                string tableName = null;
                var columns = new List<ColumnBase>();
                var primaryKeyColumnNames = new List<string>();

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    new SqlTableSchema(tableName, columns, primaryKeyColumnNames);
                });
            }

            [TestMethod]
            public void ColumnsIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var tableName = "myTableName";
                IEnumerable<ColumnBase> columns = null;
                var primaryKeyColumnNames = new List<string>();

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    new SqlTableSchema(tableName, columns, primaryKeyColumnNames);
                });
            }

            [TestMethod]
            public void PrimaryKeyColumnNamesIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var tableName = "myTableName";
                var columns = new List<ColumnBase>();
                IEnumerable<string> primaryKeyColumnNames = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    new SqlTableSchema(tableName, columns, primaryKeyColumnNames);
                });
            }

            [TestMethod]
            public void ReturnsInstance()
            {
                // Arrange
                var tableName = "myTableName";
                var columns = new List<ColumnBase>();
                var primaryKeyColumnNames = new List<string>();

                // Act
                var schema = new SqlTableSchema(tableName, columns, primaryKeyColumnNames);

                // Assert
                Assert.IsInstanceOfType(schema, typeof(SqlTableSchema));
            }

            [TestMethod]
            public void SetsTableName()
            {
                // Arrange
                var tableName = "myTableName";
                var columns = new List<ColumnBase>();
                var primaryKeyColumnNames = new List<string>();

                // Act
                var schema = new SqlTableSchema(tableName, columns, primaryKeyColumnNames);

                // Assert
                Assert.AreEqual(tableName, schema.TableName);
            }

            [TestMethod]
            public void CopiesColumns()
            {
                // Arrange
                var tableName = "myTableName";
                var columns = Helper.FakeColumns;
                var primaryKeyColumnNames = Helper.FakePrimaryKeyColumnNames;

                // Act
                var schema = new SqlTableSchema(tableName, columns, primaryKeyColumnNames);

                // Assert
                CollectionAssert.AreEqual(columns.ToList(), schema.Columns.ToList());
            }

            [TestMethod]
            public void AddsPrimaryKeyColumns()
            {
                // Arrange
                var tableName = "myTableName";
                var columns = Helper.FakeColumns;
                var primaryKeyColumnNames = Helper.FakePrimaryKeyColumnNames;

                // Act
                var schema = new SqlTableSchema(tableName, columns, primaryKeyColumnNames);

                // Assert
                schema.PrimaryKeyColumns.Single(c => c.Name == "key_part_1");
                schema.PrimaryKeyColumns.Single(c => c.Name == "key_part_2");
            }
        }
    }
}
