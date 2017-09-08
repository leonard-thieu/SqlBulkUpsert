using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    class StringColumnTests
    {
        [TestClass]
        public class Constructor
        {
            [TestMethod]
            public void SetsCharLength()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                string dataType = "myDataType";
                int? charLength = 5;
                int? byteLength = 2;

                // Act
                var column = new StringColumn(name, ordinalPosition, isNullable, dataType, charLength, byteLength);

                // Assert
                Assert.AreEqual(charLength, column.CharLength);
            }

            [TestMethod]
            public void SetsByteLength()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                string dataType = "myDataType";
                int? charLength = 5;
                int? byteLength = 2;

                // Act
                var column = new StringColumn(name, ordinalPosition, isNullable, dataType, charLength, byteLength);

                // Assert
                Assert.AreEqual(byteLength, column.ByteLength);
            }
        }

        [TestClass]
        public class ToFullDataTypeStringMethod
        {
            [DataTestMethod]
            [DataRow("char")]
            [DataRow("varchar")]
            [DataRow("nchar")]
            [DataRow("nvarchar")]
            public void DataTypeHasCharLength_ReturnsDataTypeWithCharLength(string dataType)
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                int? charLength = 5;
                int? byteLength = 2;
                var column = new StringColumn(name, ordinalPosition, isNullable, dataType, charLength, byteLength);

                // Act
                var columnDefinition = column.ToColumnDefinitionString();

                // Assert
                Assert.AreEqual($"[myName] {dataType}(5) NULL", columnDefinition);
            }

            [TestMethod]
            public void DataTypeHasCharLengthAndCharLengthIsNegative1_ReturnsDataTypeWithMaxCharLength()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                string dataType = "char";
                int? charLength = -1;
                int? byteLength = 2;
                var column = new StringColumn(name, ordinalPosition, isNullable, dataType, charLength, byteLength);

                // Act
                var columnDefinition = column.ToColumnDefinitionString();

                // Assert
                Assert.AreEqual($"[myName] {dataType}(max) NULL", columnDefinition);
            }

            [DataTestMethod]
            [DataRow("binary")]
            [DataRow("varbinary")]
            public void DataTypeHasByteLength_ReturnsDataTypeWithByteLength(string dataType)
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                int? charLength = 5;
                int? byteLength = 2;
                var column = new StringColumn(name, ordinalPosition, isNullable, dataType, charLength, byteLength);

                // Act
                var columnDefinition = column.ToColumnDefinitionString();

                // Assert
                Assert.AreEqual($"[myName] {dataType}(2) NULL", columnDefinition);
            }

            [TestMethod]
            public void DataTypeHasByteLengthAndByteLengthIsMax_ReturnsDataTypeWithMaxByteLength()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                string dataType = "binary";
                int? charLength = 5;
                int? byteLength = -1;
                var column = new StringColumn(name, ordinalPosition, isNullable, dataType, charLength, byteLength);

                // Act
                var columnDefinition = column.ToColumnDefinitionString();

                // Assert
                Assert.AreEqual($"[myName] {dataType}(max) NULL", columnDefinition);
            }

            [DataTestMethod]
            [DataRow("text")]
            [DataRow("ntext")]
            [DataRow("image")]
            public void ReturnsDataType(string dataType)
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                int? charLength = 5;
                int? byteLength = 2;
                var column = new StringColumn(name, ordinalPosition, isNullable, dataType, charLength, byteLength);

                // Act
                var columnDefinition = column.ToColumnDefinitionString();

                // Assert
                Assert.AreEqual($"[myName] {dataType} NULL", columnDefinition);
            }
        }
    }
}
