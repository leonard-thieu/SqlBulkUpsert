using System;
using System.Linq.Expressions;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    class ColumnMappingsTests
    {
        [TestClass]
        public class Constructor
        {
            [TestMethod]
            public void ReturnsInstance()
            {
                // Arrange -> Act
                var mapping = new ColumnMappings<object>("tableName");

                // Assert
                Assert.IsInstanceOfType(mapping, typeof(ColumnMappings<object>));
            }
        }

        [TestClass]
        public class Add
        {
            [TestMethod]
            public void MapIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var mapping = new ColumnMappings<object>("tableName");
                Expression<Func<object, object>> map = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    mapping.Add(map);
                });
            }
        }
    }
}
