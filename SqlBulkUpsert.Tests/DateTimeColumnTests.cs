using Xunit;

namespace SqlBulkUpsert.Tests
{
    public class DateTimeColumnTests
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

                // Act
                var column = new DateTimeColumn(name, ordinalPosition, isNullable, dataType, precision);

                // Assert
                Assert.Equal(precision, column.Precision);
            }
        }

        public class ToFullDataTypeStringMethod
        {
            [Theory]
            [InlineData("datetimeoffset")]
            [InlineData("datetime2")]
            [InlineData("time")]
            public void DataTypeHasPrecisionAndPrecisionIsNotNull_ReturnsDataTypeWithPrecision(string dataType)
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                int? precision = 5;
                var column = new DateTimeColumn(name, ordinalPosition, isNullable, dataType, precision);

                // Act
                var columnDefinition = column.ToColumnDefinitionString();

                // Assert
                Assert.Equal($"[myName] {dataType}(5) NULL", columnDefinition);
            }

            [Theory]
            [InlineData("datetimeoffset")]
            [InlineData("datetime2")]
            [InlineData("time")]
            public void DataTypeHasPrecisionAndPrecisionIsNull_ReturnsDataType(string dataType)
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                int? precision = null;
                var column = new DateTimeColumn(name, ordinalPosition, isNullable, dataType, precision);

                // Act
                var columnDefinition = column.ToColumnDefinitionString();

                // Assert
                Assert.Equal($"[myName] {dataType} NULL", columnDefinition);
            }

            [Theory]
            [InlineData("datetimeoffset")]
            [InlineData("datetime2")]
            [InlineData("time")]
            public void ReturnsDataType(string dataType)
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                int? precision = null;
                var column = new DateTimeColumn(name, ordinalPosition, isNullable, dataType, precision);

                // Act
                var columnDefinition = column.ToColumnDefinitionString();

                // Assert
                Assert.Equal($"[myName] {dataType} NULL", columnDefinition);
            }
        }
    }
}
