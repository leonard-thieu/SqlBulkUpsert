using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    class NumericColumnTests
    {
        [TestClass]
        public class Constructor
        {
            [TestMethod]
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
                Assert.AreEqual(precision, column.Precision);
            }

            [TestMethod]
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
                Assert.AreEqual(radix, column.Radix);
            }

            [TestMethod]
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
                Assert.AreEqual(scale, column.Scale);
            }
        }

        [TestClass]
        public class ToFullDataTypeStringMethod
        {
            [DataTestMethod]
            [DataRow("numeric")]
            [DataRow("decimal")]
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
                Assert.AreEqual($"[myName] {dataType}(5, 4) NULL", columnDefinition);
            }

            [DataTestMethod]
            [DataRow("float")]
            [DataRow("real")]
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
                Assert.AreEqual($"[myName] {dataType}(2) NULL", columnDefinition);
            }

            [DataTestMethod]
            [DataRow("bigint")]
            [DataRow("bit")]
            [DataRow("smallint")]
            [DataRow("smallmoney")]
            [DataRow("int")]
            [DataRow("tinyint")]
            [DataRow("money")]
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
                Assert.AreEqual($"[myName] {dataType} NULL", columnDefinition);
            }
        }
    }
}
