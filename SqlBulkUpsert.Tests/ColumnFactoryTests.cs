using System;
using System.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace SqlBulkUpsert.Tests
{
    class ColumnFactoryTests
    {
        [TestClass]
        public class CreateFromReaderMethod
        {
            [TestMethod]
            public void SqlDataReaderIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                IDataReader sqlDataReader = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    ColumnFactory.CreateFromReader(sqlDataReader);
                });
            }

            [DataTestMethod]
            [DataRow("bigint")]
            [DataRow("numeric")]
            [DataRow("bit")]
            [DataRow("smallint")]
            [DataRow("decimal")]
            [DataRow("smallmoney")]
            [DataRow("int")]
            [DataRow("tinyint")]
            [DataRow("money")]
            [DataRow("float")]
            [DataRow("real")]
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
                Assert.IsInstanceOfType(column, typeof(NumericColumn));
            }

            [DataTestMethod]
            [DataRow("date")]
            [DataRow("datetimeoffset")]
            [DataRow("datetime2")]
            [DataRow("smalldatetime")]
            [DataRow("datetime")]
            [DataRow("time")]
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
                Assert.IsInstanceOfType(column, typeof(DateTimeColumn));
            }

            [DataTestMethod]
            [DataRow("char")]
            [DataRow("varchar")]
            [DataRow("text")]
            [DataRow("nchar")]
            [DataRow("nvarchar")]
            [DataRow("ntext")]
            [DataRow("binary")]
            [DataRow("varbinary")]
            [DataRow("image")]
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
                Assert.IsInstanceOfType(column, typeof(StringColumn));
            }

            [TestMethod]
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
                Assert.ThrowsException<NotSupportedException>(() =>
                {
                    ColumnFactory.CreateFromReader(sqlDataReader);
                });
            }
        }
    }
}
