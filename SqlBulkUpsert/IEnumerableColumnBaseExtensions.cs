using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlBulkUpsert
{
    /// <summary>
    /// Contains extension methods for <see cref="IEnumerable{T}"/>.
    /// </summary>
    static class IEnumerableColumnBaseExtensions
    {
        public static string ToSelectListString(this IEnumerable<ColumnBase> columns)
        {
            if (columns == null)
                throw new ArgumentNullException(nameof(columns));

            return string.Join(", ", columns.Select(c => c.ToSelectListString()));
        }

        public static string ToColumnDefinitionListString(this IEnumerable<ColumnBase> columns)
        {
            if (columns == null)
                throw new ArgumentNullException(nameof(columns));

            return string.Join(", ", columns.Select(c => c.ToColumnDefinitionString()));
        }
    }
}