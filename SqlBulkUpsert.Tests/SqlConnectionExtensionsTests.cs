using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SqlBulkUpsert.Tests
{
    class SqlConnectionExtensionsTests
    {
        [TestClass]
        public class GetRowCountAsyncMethod
        {
            [TestMethod]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;
                var tableName = "myTableName";
                var cancellationToken = CancellationToken.None;

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.GetRowCountAsync(connection, tableName, cancellationToken);
                });
            }

            [TestMethod]
            public async Task TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                string tableName = null;
                var cancellationToken = CancellationToken.None;

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.GetRowCountAsync(connection, tableName, cancellationToken);
                });
            }
        }

        [TestClass]
        public class SwitchTableAsync
        {
            [TestMethod]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;
                var viewName = "myViewName";
                var tableName = "myTableName";
                var cancellationToken = CancellationToken.None;

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.SwitchTableAsync(connection, viewName, tableName, cancellationToken);
                });
            }

            [TestMethod]
            public async Task ViewNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                string viewName = null;
                var tableName = "myTableName";
                var cancellationToken = CancellationToken.None;

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.SwitchTableAsync(connection, viewName, tableName, cancellationToken);
                });
            }

            [TestMethod]
            public async Task TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                var viewName = "myViewName";
                string tableName = null;
                var cancellationToken = CancellationToken.None;

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.SwitchTableAsync(connection, viewName, tableName, cancellationToken);
                });
            }
        }

        [TestClass]
        public class TruncateTableAsyncMethod
        {
            [TestMethod]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;
                var tableName = "myTableName";
                var cancellationToken = CancellationToken.None;

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.TruncateTableAsync(connection, tableName, cancellationToken);
                });
            }

            [TestMethod]
            public async Task TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                string tableName = null;
                var cancellationToken = CancellationToken.None;

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.TruncateTableAsync(connection, tableName, cancellationToken);
                });
            }
        }

        [TestClass]
        public class DisableNonclusteredIndexesAsyncMethod
        {
            [TestMethod]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;
                var tableName = "myTableName";
                var cancellationToken = CancellationToken.None;

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.DisableNonclusteredIndexesAsync(connection, tableName, cancellationToken);
                });
            }

            [TestMethod]
            public async Task TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                string tableName = null;
                var cancellationToken = CancellationToken.None;

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.DisableNonclusteredIndexesAsync(connection, tableName, cancellationToken);
                });
            }
        }

        [TestClass]
        public class RebuildNonclusteredIndexesAsyncMethod
        {
            [TestMethod]
            public async Task ConnectionIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                SqlConnection connection = null;
                var tableName = "myTableName";
                var cancellationToken = CancellationToken.None;

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.RebuildNonclusteredIndexesAsync(connection, tableName, cancellationToken);
                });
            }

            [TestMethod]
            public async Task TableNameIsNull_ThrowsArgumentNullException()
            {
                // Arrange
                var connection = new SqlConnection();
                string tableName = null;
                var cancellationToken = CancellationToken.None;

                // Act -> Assert
                await Assert.ThrowsExceptionAsync<ArgumentNullException>(() =>
                {
                    return SqlConnectionExtensions.RebuildNonclusteredIndexesAsync(connection, tableName, cancellationToken);
                });
            }
        }
    }
}
