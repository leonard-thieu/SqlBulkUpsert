using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    class ColumnBaseTests
    {
        [TestClass]
        public class Constructor
        {
            [TestMethod]
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
                Assert.AreEqual(name, column.Name);
            }

            [TestMethod]
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
                Assert.AreEqual(ordinalPosition, column.OrdinalPosition);
            }

            [TestMethod]
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
                Assert.AreEqual(isNullable, column.IsNullable);
            }

            [TestMethod]
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
                Assert.AreEqual(dataType, column.DataType);
            }
        }

        [TestClass]
        public class ToSelectListStringMethod
        {
            [TestMethod]
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
                Assert.AreEqual("[myName]", select);
            }
        }

        [TestClass]
        public class ToColumnDefinitionStringMethod
        {
            [TestMethod]
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
                Assert.AreEqual("[myName] myDataType NULL", columnDefinition);
            }

            [TestMethod]
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
                Assert.AreEqual("[myName] myDataType NOT NULL", columnDefinition);
            }
        }

        [TestClass]
        public class ToFullDataTypeStringMethod
        {
            [TestMethod]
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
                Assert.AreEqual("myDataType", fullDataType);
            }
        }

        sealed class StubColumnBase : ColumnBase
        {
            public StubColumnBase(string name, int ordinalPosition, bool isNullable, string dataType) :
                base(name, ordinalPosition, isNullable, dataType)
            {

            }

            public string PublicToFullDataTypeString() => ToFullDataTypeString();
        }
    }
}
