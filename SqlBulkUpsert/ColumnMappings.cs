using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;

namespace SqlBulkUpsert
{
    public sealed class ColumnMappings<T> : Collection<KeyValuePair<string, Func<T, object>>>
    {
        public ColumnMappings(string tableName)
        {
            TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
        }

        public string TableName { get; }
        public IEnumerable<string> Columns { get => this.Select(i => i.Key); }

        public void Add(Expression<Func<T, object>> mapping)
        {
            if (mapping == null)
                throw new ArgumentNullException(nameof(mapping));

            // Get name of column
            MemberExpression operand;
            var box = mapping.Body as UnaryExpression;
            if (box != null)
            {
                // Value types need to be unboxed
                operand = (MemberExpression)box.Operand;
            }
            else
            {
                operand = (MemberExpression)mapping.Body;
            }
            var name = operand.Member.Name;

            // Get delegate
            var func = mapping.Compile();

            Add(name, func);
        }

        internal void Add(string columnName, Func<T, object> mapping)
        {
            Add(new KeyValuePair<string, Func<T, object>>(columnName, mapping));
        }
    }
}