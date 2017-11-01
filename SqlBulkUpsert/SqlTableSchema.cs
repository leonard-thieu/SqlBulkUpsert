using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlBulkUpsert
{
    internal sealed class SqlTableSchema
    {
        public SqlTableSchema(
            string tableName,
            IEnumerable<ColumnBase> columns,
            IEnumerable<string> primaryKeyColumnNames)
        {
            TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));

            if (columns == null)
                throw new ArgumentNullException(nameof(columns));
            if (primaryKeyColumnNames == null)
                throw new ArgumentNullException(nameof(primaryKeyColumnNames));

            Columns = new Columns(columns);

            var primaryKeyColumns = new List<ColumnBase>();
            foreach (var columnName in primaryKeyColumnNames)
            {
                var column = Columns.Single(c => c.Name == columnName);
                primaryKeyColumns.Add(column);
            }
            PrimaryKeyColumns = new Columns(primaryKeyColumns);
        }

        public string TableName { get; }
        public Columns Columns { get; }
        public Columns PrimaryKeyColumns { get; }
    }
}