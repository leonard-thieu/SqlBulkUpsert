using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlBulkUpsert
{
    /// <summary>
    /// Contains extension methods for <see cref="IEnumerable{T}"/>.
    /// </summary>
    static class IEnumerableColumnExtensions
    {
        public static string ToSelectListString(this IEnumerable<Column> columns)
        {
            if (columns == null)
                throw new ArgumentNullException(nameof(columns));

            return string.Join(", ", columns.Select(c => c.ToSelectListString()).ToList());
        }

        public static string ToColumnDefinitionListString(this IEnumerable<Column> columns)
        {
            if (columns == null)
                throw new ArgumentNullException(nameof(columns));

            return string.Join(", ", columns.Select(c => c.ToColumnDefinitionString()).ToList());
        }
    }
}