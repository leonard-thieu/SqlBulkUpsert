using Xunit;

namespace SqlBulkUpsert.Tests
{
    public class ColumnBaseTests
    {
        public class Constructor
        {
            [Fact]
            public void SetsName()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                string dataType = "myDataType";

                // Act
                var column = new StubColumnBase(name, ordinalPosition, isNullable, dataType);

                // Assert
                Assert.Equal(name, column.Name);
            }

            [Fact]
            public void SetsOrdinalPosition()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                string dataType = "myDataType";

                // Act
                var column = new StubColumnBase(name, ordinalPosition, isNullable, dataType);

                // Assert
                Assert.Equal(ordinalPosition, column.OrdinalPosition);
            }

            [Fact]
            public void SetsIsNullable()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                string dataType = "myDataType";

                // Act
                var column = new StubColumnBase(name, ordinalPosition, isNullable, dataType);

                // Assert
                Assert.Equal(isNullable, column.IsNullable);
            }

            [Fact]
            public void SetsDataType()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                string dataType = "myDataType";

                // Act
                var column = new StubColumnBase(name, ordinalPosition, isNullable, dataType);

                // Assert
                Assert.Equal(dataType, column.DataType);
            }
        }

        public class ToSelectListStringMethod
        {
            [Fact]
            public void ReturnsQuotedName()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                string dataType = "myDataType";
                var column = new StubColumnBase(name, ordinalPosition, isNullable, dataType);

                // Act
                var select = column.ToSelectListString();

                // Assert
                Assert.Equal("[myName]", select);
            }
        }

        public class ToColumnDefinitionStringMethod
        {
            [Fact]
            public void IsNullableIsTrue_ReturnsColumnDefinitionWithNull()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                string dataType = "myDataType";
                var column = new StubColumnBase(name, ordinalPosition, isNullable, dataType);

                // Act
                var columnDefinition = column.ToColumnDefinitionString();

                // Assert
                Assert.Equal("[myName] myDataType NULL", columnDefinition);
            }

            [Fact]
            public void IsNullableIsFalse_ReturnsColumnDefinitionWithNotNull()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = false;
                string dataType = "myDataType";
                var column = new StubColumnBase(name, ordinalPosition, isNullable, dataType);

                // Act
                var columnDefinition = column.ToColumnDefinitionString();

                // Assert
                Assert.Equal("[myName] myDataType NOT NULL", columnDefinition);
            }
        }

        public class ToFullDataTypeStringMethod
        {
            [Fact]
            public void ReturnsDataType()
            {
                // Arrange
                string name = "myName";
                int ordinalPosition = 1;
                bool isNullable = true;
                string dataType = "myDataType";
                var column = new StubColumnBase(name, ordinalPosition, isNullable, dataType);

                // Act
                var fullDataType = column.PublicToFullDataTypeString();

                // Assert
                Assert.Equal("myDataType", fullDataType);
            }
        }

        private sealed class StubColumnBase : ColumnBase
        {
            public StubColumnBase(string name, int ordinalPosition, bool isNullable, string dataType) :
                base(name, ordinalPosition, isNullable, dataType)
            {

            }

            public string PublicToFullDataTypeString() => ToFullDataTypeString();
        }
    }
}
