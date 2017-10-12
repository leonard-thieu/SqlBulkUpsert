using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

namespace SqlBulkUpsert
{
    sealed class SqlTableSchema
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

            foreach (var column in columns)
            {
                Columns.Add(column);
            }

            foreach (var columnName in primaryKeyColumnNames)
            {
                var column = Columns.Single(c => c.Name == columnName);
                PrimaryKeyColumns.Add(column);
            }
        }

        public string TableName { get; }
        public Collection<ColumnBase> Columns { get; } = new Collection<ColumnBase>();
        public Collection<ColumnBase> PrimaryKeyColumns { get; } = new Collection<ColumnBase>();
    }
}