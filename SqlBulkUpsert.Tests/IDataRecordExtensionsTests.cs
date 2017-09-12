using System;
using System.Data;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Moq;

namespace SqlBulkUpsert.Tests
{
    class IDataRecordExtensionsTests
    {
        [TestClass]
        public class GetValueMethod
        {
            [TestMethod]
            public void ReaderIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                IDataRecord reader = null;
                var columnName = "myColumnName";

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    IDataRecordExtensions.GetValue<int>(reader, columnName);
                });
            }

            [TestMethod]
            public void ColumnNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var reader = Mock.Of<IDataRecord>();
                string columnName = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    IDataRecordExtensions.GetValue<int>(reader, columnName);
                });
            }

            [TestMethod]
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
                Assert.IsNull(value);
            }

            [TestMethod]
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
                Assert.AreEqual(20, value);
            }
        }
    }
}
