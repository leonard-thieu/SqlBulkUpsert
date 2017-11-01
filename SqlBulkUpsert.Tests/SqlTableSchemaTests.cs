using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace SqlBulkUpsert.Tests
{
    public class SqlTableSchemaTests
    {
        public class Cosntructor
        {
            [Fact]
            public void TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                string tableName = null;
                var columns = new List<ColumnBase>();
                var primaryKeyColumnNames = new List<string>();

                // Act -> Assert
                Assert.Throws<ArgumentNullException>(() =>
                {
                    new SqlTableSchema(tableName, columns, primaryKeyColumnNames);
                });
            }

            [Fact]
            public void ColumnsIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var tableName = "myTableName";
                IEnumerable<ColumnBase> columns = null;
                var primaryKeyColumnNames = new List<string>();

                // Act -> Assert
                Assert.Throws<ArgumentNullException>(() =>
                {
                    new SqlTableSchema(tableName, columns, primaryKeyColumnNames);
                });
            }

            [Fact]
            public void PrimaryKeyColumnNamesIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var tableName = "myTableName";
                var columns = new List<ColumnBase>();
                IEnumerable<string> primaryKeyColumnNames = null;

                // Act -> Assert
                Assert.Throws<ArgumentNullException>(() =>
                {
                    new SqlTableSchema(tableName, columns, primaryKeyColumnNames);
                });
            }

            [Fact]
            public void ReturnsInstance()
            {
                // Arrange
                var tableName = "myTableName";
                var columns = new List<ColumnBase>();
                var primaryKeyColumnNames = new List<string>();

                // Act
                var schema = new SqlTableSchema(tableName, columns, primaryKeyColumnNames);

                // Assert
                Assert.IsAssignableFrom<SqlTableSchema>(schema);
            }

            [Fact]
            public void SetsTableName()
            {
                // Arrange
                var tableName = "myTableName";
                var columns = new List<ColumnBase>();
                var primaryKeyColumnNames = new List<string>();

                // Act
                var schema = new SqlTableSchema(tableName, columns, primaryKeyColumnNames);

                // Assert
                Assert.Equal(tableName, schema.TableName);
            }

            [Fact]
            public void CopiesColumns()
            {
                // Arrange
                var tableName = "myTableName";
                var columns = Helper.FakeColumns;
                var primaryKeyColumnNames = Helper.FakePrimaryKeyColumnNames;

                // Act
                var schema = new SqlTableSchema(tableName, columns, primaryKeyColumnNames);

                // Assert
                Assert.Equal(columns.ToList(), schema.Columns.ToList());
            }

            [Fact]
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
