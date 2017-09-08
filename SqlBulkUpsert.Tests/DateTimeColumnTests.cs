using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    class DateTimeColumnTests
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

                // Act
                var column = new DateTimeColumn(name, ordinalPosition, isNullable, dataType, precision);

                // Assert
                Assert.AreEqual(precision, column.Precision);
            }
        }

        [TestClass]
        public class ToFullDataTypeStringMethod
        {
            [DataTestMethod]
            [DataRow("datetimeoffset")]
            [DataRow("datetime2")]
            [DataRow("time")]
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
                Assert.AreEqual($"[myName] {dataType}(5) NULL", columnDefinition);
            }

            [DataTestMethod]
            [DataRow("datetimeoffset")]
            [DataRow("datetime2")]
            [DataRow("time")]
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
                Assert.AreEqual($"[myName] {dataType} NULL", columnDefinition);
            }

            [DataTestMethod]
            [DataRow("datetimeoffset")]
            [DataRow("datetime2")]
            [DataRow("time")]
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
                Assert.AreEqual($"[myName] {dataType} NULL", columnDefinition);
            }
        }
    }
}
