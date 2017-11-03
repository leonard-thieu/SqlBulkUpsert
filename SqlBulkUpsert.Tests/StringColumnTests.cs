using Xunit;

namespace SqlBulkUpsert.Tests
{
    public class StringColumnTests
    {
        public class Constructor
        {
            [Fact]
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
                Assert.Equal(charLength, column.CharLength);
            }

            [Fact]
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
                Assert.Equal(byteLength, column.ByteLength);
            }
        }

        public class ToFullDataTypeStringMethod
        {
            [Theory]
            [InlineData("char")]
            [InlineData("varchar")]
            [InlineData("nchar")]
            [InlineData("nvarchar")]
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
                Assert.Equal($"[myName] {dataType}(5) NULL", columnDefinition);
            }

            [Fact]
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
                Assert.Equal($"[myName] {dataType}(max) NULL", columnDefinition);
            }

            [Theory]
            [InlineData("binary")]
            [InlineData("varbinary")]
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
                Assert.Equal($"[myName] {dataType}(2) NULL", columnDefinition);
            }

            [Fact]
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
                Assert.Equal($"[myName] {dataType}(max) NULL", columnDefinition);
            }

            [Theory]
            [InlineData("text")]
            [InlineData("ntext")]
            [InlineData("image")]
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
                Assert.Equal($"[myName] {dataType} NULL", columnDefinition);
            }
        }
    }
}
