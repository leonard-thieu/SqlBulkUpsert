﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace SqlBulkUpsert.Tests
{
    public class SqlConnectionExtensionsTests
    {
        public class UseAsyncMethod
        {
            [Fact]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;
                var databaseName = "myDatabase";

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.UseAsync(connection, databaseName);
                });
            }

            [Fact]
            public async Task DatabaseNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                string databaseName = null;

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.UseAsync(connection, databaseName);
                });
            }
        }

        public class GetUseCommandMethod
        {
            [Fact]
            public void ReturnsUseCommand()
            {
                // Arrange
                var connection = new SqlConnection();
                var databaseName = "myDatabase";

                // Act
                var command = SqlConnectionExtensions.GetUseCommand(connection, databaseName);

                // Assert
                Assert.Equal("USE [myDatabase];", command.CommandText);
            }
        }

        public class SelectTableSchemaAsyncMethod
        {
            [Fact]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;
                var tableName = "myTableName";

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.SelectTableSchemaAsync(connection, tableName);
                });
            }

            [Fact]
            public async Task TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                string tableName = null;

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.SelectTableSchemaAsync(connection, tableName);
                });
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
                public async Task SchemDoesnotExist_ThrowsInvalidOperationException()
                {
                    // Arrange
                    using (var connection = await DatabaseHelper.CreateAndOpenConnectionAsync())
                    {
                        // Act -> Assert
                        await Assert.ThrowsAsync<InvalidOperationException>(() =>
                        {
                            return connection.SelectTableSchemaAsync("DoesNotExist");
                        });
                    }
                }

                [Fact]
                public async Task ReturnsTableSchema()
                {
                    // Arrange
                    using (var connection = await DatabaseHelper.CreateAndOpenConnectionAsync(Constants.DatabaseName))
                    {
                        var expectedColumns = new List<ColumnBase>
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

                        var expectedKeyColumns = new List<ColumnBase>
                        {
                            new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
                            new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
                        };

                        // Act
                        var schema = await connection.SelectTableSchemaAsync(Constants.TableName);

                        // Assert
                        Assert.Equal(Constants.TableName, schema.TableName);
                        Assert.Equal(expectedColumns, schema.Columns.ToList(), new ColumnEqualityComparer());
                        Assert.Equal(expectedKeyColumns, schema.PrimaryKeyColumns.ToList(), new ColumnEqualityComparer());
                    }
                }
            }
        }

        public class GetSelectTableSchemaCommandMethod
        {
            [Fact]
            public void ReturnsSelectTableSchemaCommand()
            {
                // Arrange
                var connection = new SqlConnection();
                var tableName = "myTableName";

                // Act
                var command = SqlConnectionExtensions.GetSelectTableSchemaCommand(connection, tableName);

                // Assert
                Assert.Equal(@"-- Check table exists
SELECT *
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME = @tableName;

-- Get column schema information for table (need this to create our temp table)
SELECT *
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = @tableName;

-- Identifies the columns making up the primary key (do we use this for our match?)
SELECT kcu.COLUMN_NAME
FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
    ON kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
    AND CONSTRAINT_TYPE = 'PRIMARY KEY'
WHERE kcu.TABLE_NAME = @tableName;", command.CommandText, ignoreLineEndingDifferences: true);
                Assert.True(command.Parameters.Contains("@tableName"));
            }
        }

        public class ReadTableSchemaAsyncMethod
        {
            [Fact]
            public async Task ReadsTableSchema()
            {
                // Arrange
                var tableName = Constants.TableName;

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

                var expectedColumns = new List<ColumnBase>
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

                var expectedKeyColumns = new List<ColumnBase>
                {
                    new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
                    new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
                };

                var cancellationToken = CancellationToken.None;

                // Act
                var schema = await SqlConnectionExtensions.ReadTableSchemaAsync(dataTableReader, tableName, cancellationToken);

                // Assert 
                Assert.Equal(tableName, schema.TableName);
                Assert.Equal(expectedColumns, schema.Columns.ToList(), new ColumnEqualityComparer());
                Assert.Equal(expectedKeyColumns, schema.PrimaryKeyColumns.ToList(), new ColumnEqualityComparer());
            }
        }

        public class CountAsyncMethod
        {
            [Fact]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;
                var tableName = "myTableName";

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.CountAsync(connection, tableName);
                });
            }

            [Fact]
            public async Task TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                string tableName = null;

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.CountAsync(connection, tableName);
                });
            }
        }

        public class GetCountCommandMethod
        {
            [Fact]
            public void ReturnsCountCommand()
            {
                // Arrange
                var connection = new SqlConnection();
                var tableName = "myTableName";

                // Act
                var command = SqlConnectionExtensions.GetCountCommand(connection, tableName);

                // Assert
                Assert.Equal(@"SELECT Count(*) 
FROM [myTableName];", command.CommandText, ignoreLineEndingDifferences: true);
            }
        }

        public class SwitchTableAsync
        {
            [Fact]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;
                var viewName = "myViewName";
                var tableName = "myTableName";
                var columns = new Columns(new List<ColumnBase>());

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.SwitchTableAsync(connection, viewName, tableName, columns);
                });
            }

            [Fact]
            public async Task ViewNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                string viewName = null;
                var tableName = "myTableName";
                var columns = new Columns(new List<ColumnBase>());

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.SwitchTableAsync(connection, viewName, tableName, columns);
                });
            }

            [Fact]
            public async Task TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                var viewName = "myViewName";
                string tableName = null;
                var columns = new Columns(new List<ColumnBase>());

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.SwitchTableAsync(connection, viewName, tableName, columns);
                });
            }
        }

        public class GetSwitchTableCommandMethod
        {
            [Fact]
            public void ReturnsSwitchTableCommand()
            {
                // Arrange
                var connection = new SqlConnection();
                var viewName = "myViewName";
                var tableName = "myTableName";
                var columns = new Columns(new List<ColumnBase>
                {
                    new StringColumn("key_part_1", 1, false, "nchar", 4, 8),
                    new NumericColumn("key_part_2", 2, false, "smallint", 5, 10, 0),
                    new StringColumn("nullable_text", 3, true, "nvarchar", 50, 100),
                    new NumericColumn("nullable_number", 4, true, "int", 10, 10, 0),
                    new DateTimeColumn("nullable_datetimeoffset", 5, true, "datetimeoffset", 7),
                    new NumericColumn("nullable_money", 6, true, "money", 19, 10, 4),
                    new StringColumn("nullable_varbinary", 7, true, "varbinary", -1, -1),
                    new StringColumn("nullable_image", 8, true, "image", 2147483647, 2147483647),
                });

                // Act
                var command = SqlConnectionExtensions.GetSwitchTableCommand(connection, viewName, tableName, columns);

                // Assert
                Assert.Equal(@"ALTER VIEW [myViewName]
AS

SELECT [key_part_1], [key_part_2], [nullable_text], [nullable_number], [nullable_datetimeoffset], [nullable_money], [nullable_varbinary], [nullable_image]
FROM [myTableName];", command.CommandText, ignoreLineEndingDifferences: true);
            }
        }

        public class TruncateTableAsyncMethod
        {
            [Fact]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;
                var tableName = "myTableName";

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.TruncateTableAsync(connection, tableName);
                });
            }

            [Fact]
            public async Task TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                string tableName = null;

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.TruncateTableAsync(connection, tableName);
                });
            }
        }

        public class GetTruncateTableCommand
        {
            [Fact]
            public void ReturnsTruncateTableCommand()
            {
                // Arrange
                var connection = new SqlConnection();
                var tableName = "myTableName";

                // Act
                var command = SqlConnectionExtensions.GetTruncateTableCommand(connection, tableName);

                // Assert
                Assert.Equal("TRUNCATE TABLE [myTableName];", command.CommandText);
            }
        }

        public class SelectIntoTemporaryTableAsyncMethod
        {
            [Fact]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;
                var baseTableName = "myTableName";
                var tempTableName = "#myTableName";

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.SelectIntoTemporaryTableAsync(connection, baseTableName, tempTableName);
                });
            }

            [Fact]
            public async Task BaseTableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                string baseTableName = null;
                var tempTableName = "#myTableName";

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.SelectIntoTemporaryTableAsync(connection, baseTableName, tempTableName);
                });
            }

            [Fact]
            public async Task TempTableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                var baseTableName = "myTableName";
                string tempTableName = null;

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.SelectIntoTemporaryTableAsync(connection, baseTableName, tempTableName);
                });
            }
        }

        public class GetSelectIntoTemporaryTableCommandMethod
        {
            [Fact]
            public void ReturnsSelectIntoTemporaryTableCommand()
            {
                // Arrange
                var connection = new SqlConnection();
                var baseTableName = "myTableName";
                var tempTableName = "#myTableName";

                // Act
                var comnand = SqlConnectionExtensions.GetSelectIntoTemporaryTableCommand(connection, baseTableName, tempTableName);

                // Assert
                Assert.Equal("SELECT TOP 0 * INTO [#myTableName] FROM [myTableName];", comnand.CommandText);
            }
        }

        public class MergeAsyncMethod
        {
            [Fact]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;
                var tableSource = "myTableName";
                var targetTableSchema = Helper.FakeSqlTableSchema;
                var updateWhenMatched = true;

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.MergeAsync(connection, tableSource, targetTableSchema, updateWhenMatched);
                });
            }

            [Fact]
            public async Task TableSourceIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                string tableSource = null;
                var targetTableSchema = Helper.FakeSqlTableSchema;
                var updateWhenMatched = true;

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.MergeAsync(connection, tableSource, targetTableSchema, updateWhenMatched);
                });
            }

            [Fact]
            public async Task TargetTableSchemaIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                var tableSource = "myTableName";
                SqlTableSchema targetTableSchema = null;
                var updateWhenMatched = true;

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.MergeAsync(connection, tableSource, targetTableSchema, updateWhenMatched);
                });
            }
        }

        public class GetMergeCommandMethod
        {
            [Fact]
            public void UpdateWhenMatchedIsTrue_ReturnsMergeCommandWithUpdateWhenMatched()
            {
                // Arrange
                var connection = new SqlConnection();
                var tableSource = "myTableSource";
                var targetTableSchema = Helper.FakeSqlTableSchema;
                var updateWhenMatched = true;

                // Act
                var command = SqlConnectionExtensions.GetMergeCommand(connection, tableSource, targetTableSchema, updateWhenMatched);

                // Assert
                Assert.Equal(@"MERGE INTO [myTableName] AS [Target]
USING [myTableSource] AS [Source]
    ON ([Target].[key_part_1] = [Source].[key_part_1] AND [Target].[key_part_2] = [Source].[key_part_2])
WHEN MATCHED
    THEN
        UPDATE
        SET [Target].[nullable_text] = [Source].[nullable_text],
            [Target].[nullable_number] = [Source].[nullable_number],
            [Target].[nullable_datetimeoffset] = [Source].[nullable_datetimeoffset],
            [Target].[nullable_money] = [Source].[nullable_money],
            [Target].[nullable_varbinary] = [Source].[nullable_varbinary],
            [Target].[nullable_image] = [Source].[nullable_image]
WHEN NOT MATCHED
    THEN
        INSERT ([key_part_1], [key_part_2], [nullable_text], [nullable_number], [nullable_datetimeoffset], [nullable_money], [nullable_varbinary], [nullable_image])
        VALUES ([key_part_1], [key_part_2], [nullable_text], [nullable_number], [nullable_datetimeoffset], [nullable_money], [nullable_varbinary], [nullable_image]);
", command.CommandText, ignoreLineEndingDifferences: true);
            }

            [Fact]
            public void UpdateWhenMatchedIsFalse_ReturnsMergeCommand()
            {
                // Arrange
                var connection = new SqlConnection();
                var tableSource = "myTableSource";
                var targetTableSchema = Helper.FakeSqlTableSchema;
                var updateWhenMatched = false;

                // Act
                var command = SqlConnectionExtensions.GetMergeCommand(connection, tableSource, targetTableSchema, updateWhenMatched);

                // Assert
                Assert.Equal(@"MERGE INTO [myTableName] AS [Target]
USING [myTableSource] AS [Source]
    ON ([Target].[key_part_1] = [Source].[key_part_1] AND [Target].[key_part_2] = [Source].[key_part_2])
WHEN NOT MATCHED
    THEN
        INSERT ([key_part_1], [key_part_2], [nullable_text], [nullable_number], [nullable_datetimeoffset], [nullable_money], [nullable_varbinary], [nullable_image])
        VALUES ([key_part_1], [key_part_2], [nullable_text], [nullable_number], [nullable_datetimeoffset], [nullable_money], [nullable_varbinary], [nullable_image]);
", command.CommandText, ignoreLineEndingDifferences: true);
            }
        }

        public class DisableNonclusteredIndexesAsyncMethod
        {
            [Fact]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;
                var tableName = "myTableName";

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.DisableNonclusteredIndexesAsync(connection, tableName);
                });
            }

            [Fact]
            public async Task TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                string tableName = null;

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.DisableNonclusteredIndexesAsync(connection, tableName);
                });
            }
        }

        public class RebuildNonclusteredIndexesAsyncMethod
        {
            [Fact]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;
                var tableName = "myTableName";

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.RebuildNonclusteredIndexesAsync(connection, tableName);
                });
            }

            [Fact]
            public async Task TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                string tableName = null;

                // Act -> Assert
                await Assert.ThrowsAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.RebuildNonclusteredIndexesAsync(connection, tableName);
                });
            }
        }

        public class GetAlterNonclusteredIndexesCommand
        {
            [Fact]
            public void ReturnsAlterNonclusteredIndexesCommand()
            {
                // Arrange
                var connection = new SqlConnection();
                var tableName = "myTableName";
                var action = "DISABLE";

                // Act
                var command = SqlConnectionExtensions.GetAlterNonclusteredIndexesCommand(connection, tableName, action);

                // Assert
                Assert.Equal(@"DECLARE @sql AS VARCHAR(MAX)='';

SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' DISABLE;' + CHAR(13) + CHAR(10)
FROM sys.indexes
JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id
WHERE sys.indexes.type_desc = 'NONCLUSTERED'
  AND sys.objects.type_desc = 'USER_TABLE'
  AND sys.objects.name = @tableName;

EXEC(@sql);", command.CommandText, ignoreLineEndingDifferences: true);
                Assert.True(command.Parameters.Contains("@tableName"));
            }
        }
    }
}
