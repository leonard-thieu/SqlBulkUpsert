using Xunit;

namespace SqlBulkUpsert.Tests
{
    public class NumericColumnTests
    {
        public class Constructor
        {
            [Fact]
            public void SetsPrecision()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                string dataType = "myDataType";
                int? precision = 5;
                int? radix = 2;
                int? scale = 4;

                // Act
                var column = new NumericColumn(name, ordinalPosition, isNullable, dataType, precision, radix, scale);

                // Assert
                Assert.Equal(precision, column.Precision);
            }

            [Fact]
            public void SetsRadix()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                string dataType = "myDataType";
                int? precision = 5;
                int? radix = 2;
                int? scale = 4;

                // Act
                var column = new NumericColumn(name, ordinalPosition, isNullable, dataType, precision, radix, scale);

                // Assert
                Assert.Equal(radix, column.Radix);
            }

            [Fact]
            public void SetsScale()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                string dataType = "myDataType";
                int? precision = 5;
                int? radix = 2;
                int? scale = 4;

                // Act
                var column = new NumericColumn(name, ordinalPosition, isNullable, dataType, precision, radix, scale);

                // Assert
                Assert.Equal(scale, column.Scale);
            }
        }

        public class ToFullDataTypeStringMethod
        {
            [Theory]
            [InlineData("numeric")]
            [InlineData("decimal")]
            public void DataTypeHasPrecisionAndScale_ReturnsDataTypeWithPrecisionAndScale(string dataType)
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                int? precision = 5;
                int? radix = 2;
                int? scale = 4;
                var column = new NumericColumn(name, ordinalPosition, isNullable, dataType, precision, radix, scale);

                // Act
                var columnDefinition = column.ToColumnDefinitionString();

                // Assert
                Assert.Equal($"[myName] {dataType}(5, 4) NULL", columnDefinition);
            }

            [Theory]
            [InlineData("float")]
            [InlineData("real")]
            public void DataTypeHasRadix_ReturnsDataTypeWithRadix(string dataType)
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                int? precision = 5;
                int? radix = 2;
                int? scale = 4;
                var column = new NumericColumn(name, ordinalPosition, isNullable, dataType, precision, radix, scale);

                // Act
                var columnDefinition = column.ToColumnDefinitionString();

                // Assert
                Assert.Equal($"[myName] {dataType}(2) NULL", columnDefinition);
            }

            [Theory]
            [InlineData("bigint")]
            [InlineData("bit")]
            [InlineData("smallint")]
            [InlineData("smallmoney")]
            [InlineData("int")]
            [InlineData("tinyint")]
            [InlineData("money")]
            public void ReturnsDataType(string dataType)
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                int? precision = 5;
                int? radix = 2;
                int? scale = 4;
                var column = new NumericColumn(name, ordinalPosition, isNullable, dataType, precision, radix, scale);

                // Act
                var columnDefinition = column.ToColumnDefinitionString();

                // Assert
                Assert.Equal($"[myName] {dataType} NULL", columnDefinition);
            }
        }
    }
}
