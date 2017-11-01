using System;
using System.Data;
using Moq;
using Xunit;

namespace SqlBulkUpsert.Tests
{
    public class IDataRecordExtensionsTests
    {
        public class GetValueMethod
        {
            [Fact]
            public void ReaderIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                IDataRecord reader = null;
                var columnName = "myColumnName";

                // Act -> Assert
                Assert.Throws<ArgumentNullException>(() =>
                {
                    IDataRecordExtensions.GetValue<int>(reader, columnName);
                });
            }

            [Fact]
            public void ColumnNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var reader = Mock.Of<IDataRecord>();
                string columnName = null;

                // Act -> Assert
                Assert.Throws<ArgumentNullException>(() =>
                {
                    IDataRecordExtensions.GetValue<int>(reader, columnName);
                });
            }

            [Fact]
            public void FieldIsDBNull_ReturnsDefaultValue()
            {
                // Arrange
                var mockReader = new Mock<IDataRecord>();
                mockReader.Setup(r => r.IsDBNull(It.IsAny<int>())).Returns(true);
                var reader = mockReader.Object;
                var columnName = "myColumnName";

                // Act
                var value = IDataRecordExtensions.GetValue<int?>(reader, columnName);

                // Assert
                Assert.Null(value);
            }

            [Fact]
            public void FieldIsNotDBNull_ReturnsValue()
            {
                // Arrange
                var mockReader = new Mock<IDataRecord>();
                mockReader.Setup(r => r.IsDBNull(It.IsAny<int>())).Returns(false);
                mockReader.Setup(r => r.GetValue(It.IsAny<int>())).Returns(20);
                var reader = mockReader.Object;
                var columnName = "myColumnName";

                // Act
                var value = IDataRecordExtensions.GetValue<int?>(reader, columnName);

                // Assert
                Assert.Equal(20, value);
            }
        }
    }
}
