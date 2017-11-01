using System;
using System.Data;
using Moq;
using Xunit;

namespace SqlBulkUpsert.Tests
{
    public class ColumnFactoryTests
    {
        public class CreateFromReaderMethod
        {
            [Fact]
            public void SqlDataReaderIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                IDataReader sqlDataReader = null;

                // Act -> Assert
                Assert.Throws<ArgumentNullException>(() =>
                {
                    ColumnFactory.CreateFromReader(sqlDataReader);
                });
            }

            [Theory]
            [InlineData("bigint")]
            [InlineData("numeric")]
            [InlineData("bit")]
            [InlineData("smallint")]
            [InlineData("decimal")]
            [InlineData("smallmoney")]
            [InlineData("int")]
            [InlineData("tinyint")]
            [InlineData("money")]
            [InlineData("float")]
            [InlineData("real")]
            public void DataTypeIsNumeric_ReturnsNumericColumn(string dataType)
            {
                // Arrange
                Mock<IDataReader> mockSqlDataReader = new Mock<IDataReader>();
                mockSqlDataReader
                    .SetupGet(r => r["COLUMN_NAME"])
                    .Returns("myColumnName");
                mockSqlDataReader
                    .SetupGet(r => r["ORDINAL_POSITION"])
                    .Returns(1);
                mockSqlDataReader
                    .SetupGet(r => r["IS_NULLABLE"])
                    .Returns("YES");
                mockSqlDataReader
                    .SetupGet(r => r["DATA_TYPE"])
                    .Returns(dataType);
                IDataReader sqlDataReader = mockSqlDataReader.Object;

                // Act
                var column = ColumnFactory.CreateFromReader(sqlDataReader);

                // Assert
                Assert.IsAssignableFrom<NumericColumn>(column);
            }

            [Theory]
            [InlineData("date")]
            [InlineData("datetimeoffset")]
            [InlineData("datetime2")]
            [InlineData("smalldatetime")]
            [InlineData("datetime")]
            [InlineData("time")]
            public void DataTypeIsDateTime_ReturnsDateTimeColumn(string dataType)
            {
                // Arrange
                Mock<IDataReader> mockSqlDataReader = new Mock<IDataReader>();
                mockSqlDataReader
                    .SetupGet(r => r["COLUMN_NAME"])
                    .Returns("myColumnName");
                mockSqlDataReader
                    .SetupGet(r => r["ORDINAL_POSITION"])
                    .Returns(1);
                mockSqlDataReader
                    .SetupGet(r => r["IS_NULLABLE"])
                    .Returns("YES");
                mockSqlDataReader
                    .SetupGet(r => r["DATA_TYPE"])
                    .Returns(dataType);
                IDataReader sqlDataReader = mockSqlDataReader.Object;

                // Act
                var column = ColumnFactory.CreateFromReader(sqlDataReader);

                // Assert
                Assert.IsAssignableFrom<DateTimeColumn>(column);
            }

            [Theory]
            [InlineData("char")]
            [InlineData("varchar")]
            [InlineData("text")]
            [InlineData("nchar")]
            [InlineData("nvarchar")]
            [InlineData("ntext")]
            [InlineData("binary")]
            [InlineData("varbinary")]
            [InlineData("image")]
            public void DataTypeIsString_ReturnsStringColumn(string dataType)
            {
                // Arrange
                Mock<IDataReader> mockSqlDataReader = new Mock<IDataReader>();
                mockSqlDataReader
                    .SetupGet(r => r["COLUMN_NAME"])
                    .Returns("myColumnName");
                mockSqlDataReader
                    .SetupGet(r => r["ORDINAL_POSITION"])
                    .Returns(1);
                mockSqlDataReader
                    .SetupGet(r => r["IS_NULLABLE"])
                    .Returns("YES");
                mockSqlDataReader
                    .SetupGet(r => r["DATA_TYPE"])
                    .Returns(dataType);
                IDataReader sqlDataReader = mockSqlDataReader.Object;

                // Act
                var column = ColumnFactory.CreateFromReader(sqlDataReader);

                // Assert
                Assert.IsAssignableFrom<StringColumn>(column);
            }

            [Fact]
            public void DataTypeIsNotSupported_ThrowsNotSupportedException()
            {
                // Arrange
                string dataType = "xml";
                Mock<IDataReader> mockSqlDataReader = new Mock<IDataReader>();
                mockSqlDataReader
                    .SetupGet(r => r["COLUMN_NAME"])
                    .Returns("myColumnName");
                mockSqlDataReader
                    .SetupGet(r => r["ORDINAL_POSITION"])
                    .Returns(1);
                mockSqlDataReader
                    .SetupGet(r => r["IS_NULLABLE"])
                    .Returns("YES");
                mockSqlDataReader
                    .SetupGet(r => r["DATA_TYPE"])
                    .Returns(dataType);
                IDataReader sqlDataReader = mockSqlDataReader.Object;

                // Act -> Assert
                Assert.Throws<NotSupportedException>(() =>
                {
                    ColumnFactory.CreateFromReader(sqlDataReader);
                });
            }
        }
    }
}
