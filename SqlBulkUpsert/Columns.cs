using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SqlBulkUpsert
{
    sealed class Columns : IEnumerable<ColumnBase>
    {
        public Columns(IEnumerable<ColumnBase> columns)
        {
            if (columns == null)
                throw new ArgumentNullException(nameof(columns));

            this.columns = columns.ToList();
        }

        readonly List<ColumnBase> columns;

        public string ToSelectListString() => string.Join(", ", columns.Select(c => c.ToSelectListString()));

        // TODO: This isn't used anymore. What was it originally used for?
        public string ToColumnDefinitionListString() => string.Join(", ", columns.Select(c => c.ToColumnDefinitionString()));

        public IEnumerator<ColumnBase> GetEnumerator() => columns.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => columns.GetEnumerator();
    }
}
