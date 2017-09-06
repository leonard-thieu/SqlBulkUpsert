using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using toofz.TestsShared;

namespace SqlBulkUpsert.Tests
{
    class SqlCommandExceptionTests
    {
        [TestClass]
        public class Constructor
        {
            [TestMethod]
            public void ReturnsInstance()
            {
                // Arrange
                string message = null;
                Exception inner = null;
                string commandText = null;

                // Act
                var ex = new SqlCommandException(message, inner, commandText);

                // Assert
                Assert.IsInstanceOfType(ex, typeof(SqlCommandException));
            }

            [TestMethod]
            public void SetsCommandText()
            {
                // Arrange
                string message = null;
                Exception inner = null;
                string commandText = "myCommandText";

                // Act
                var ex = new SqlCommandException(message, inner, commandText);

                // Assert
                Assert.AreEqual(commandText, ex.CommandText);
            }
        }

        [TestClass]
        public new class ToString
        {
            [TestMethod]
            public void CommandTextIsNull_ReturnsSqlCommandExceptionAsString()
            {
                // Arrange
                string message = null;
                Exception inner = null;
                string commandText = null;

                // Act
                var ex = new SqlCommandException(message, inner, commandText);

                // Assert
                Assert.AreEqual("SqlBulkUpsert.SqlCommandException: Exception of type 'SqlBulkUpsert.SqlCommandException' was thrown.", ex.ToString());
            }

            [TestMethod]
            public void ReturnsSqlCommandExceptionAsString()
            {
                // Arrange
                string message = null;
                Exception inner = null;
                string commandText = "myCommandText";

                // Act
                var ex = new SqlCommandException(message, inner, commandText);

                // Assert
                Assert.That.NormalizedAreEqual(@"SqlBulkUpsert.SqlCommandException: Exception of type 'SqlBulkUpsert.SqlCommandException' was thrown.

myCommandText", ex.ToString());
            }
        }
    }
}
