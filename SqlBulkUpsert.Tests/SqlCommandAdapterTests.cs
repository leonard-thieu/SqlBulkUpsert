using System;
using System.Data.SqlClient;
using Xunit;

namespace SqlBulkUpsert.Tests
{
    public class SqlCommandAdapterTests
    {
        public class FromConnectionMethod
        {
            [Fact]
            public void ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;

                // Act -> Assert
                Assert.Throws<ArgumentNullException>(() =>
                {
                    SqlCommandAdapter.FromConnection(connection);
                });
            }

            [Fact]
            public void ReturnsInstance()
            {
                // Arrange
                var connection = new SqlConnection();

                // Act
                var adapter = SqlCommandAdapter.FromConnection(connection);

                // Assert
                Assert.IsAssignableFrom<SqlCommandAdapter>(adapter);
            }
        }

        public class CommandTextProperty
        {
            [Fact]
            public void GetSetBehavior()
            {
                // Arrange
                var connection = new SqlConnection();
                var adapter = SqlCommandAdapter.FromConnection(connection);

                // Act -> Assert
                adapter.CommandText = "myCommandText";
                Assert.Equal("myCommandText", adapter.CommandText);
            }
        }

        public class ParametersProperty
        {
            [Fact]
            public void ReturnsInstance()
            {
                // Arrange
                var connection = new SqlConnection();
                var adapter = SqlCommandAdapter.FromConnection(connection);

                // Act
                var parameters = adapter.Parameters;

                // Assert
                Assert.IsAssignableFrom<SqlParameterCollection>(parameters);
            }
        }
    }
}
