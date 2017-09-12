using System;
using System.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    class SqlCommandAdapterTests
    {
        [TestClass]
        public class FromConnectionMethod
        {
            [TestMethod]
            public void ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;

                // Act -> Assert
                Assert.ThrowsException<ArgumentNullException>(() =>
                {
                    SqlCommandAdapter.FromConnection(connection);
                });
            }

            [TestMethod]
            public void ReturnsInstance()
            {
                // Arrange
                var connection = new SqlConnection();

                // Act
                var adapter = SqlCommandAdapter.FromConnection(connection);

                // Assert
                Assert.IsInstanceOfType(adapter, typeof(SqlCommandAdapter));
            }
        }

        [TestClass]
        public class CommandTextProperty
        {
            [TestMethod]
            public void GetSetBehavior()
            {
                // Arrange
                var connection = new SqlConnection();
                var adapter = SqlCommandAdapter.FromConnection(connection);

                // Act -> Assert
                adapter.CommandText = "myCommandText";
                Assert.AreEqual("myCommandText", adapter.CommandText);
            }
        }

        [TestClass]
        public class ParametersProperty
        {
            [TestMethod]
            public void ReturnsInstance()
            {
                // Arrange
                var connection = new SqlConnection();
                var adapter = SqlCommandAdapter.FromConnection(connection);

                // Act
                var parameters = adapter.Parameters;

                // Assert
                Assert.IsInstanceOfType(parameters, typeof(SqlParameterCollection));
            }
        }
    }
}
