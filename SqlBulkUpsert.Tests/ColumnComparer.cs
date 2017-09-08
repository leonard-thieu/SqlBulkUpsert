using System;
using System.Collections.Generic;

namespace SqlBulkUpsert.Tests
{
    sealed class ColumnComparer : Comparer<Column>
    {
        static bool Equals(NumericColumn x, NumericColumn y)
        {
            return
                x.Precision == y.Precision &&
                x.Radix == y.Radix &&
                x.Scale == y.Scale &&
                Equals((Column)x, (Column)y);
        }

        static bool Equals(DateTimeColumn x, DateTimeColumn y)
        {
            return
                x.Precision == y.Precision &&
                Equals((Column)x, (Column)y);
        }

        static bool Equals(StringColumn x, StringColumn y)
        {
            return
                x.CharLength == y.CharLength &&
                x.ByteLength == y.ByteLength &&
                Equals((Column)x, (Column)y);
        }

        static bool Equals(Column x, Column y)
        {
            return
                x.Name == y.Name &&
                x.OrdinalPosition == y.OrdinalPosition &&
                x.IsNullable == y.IsNullable &&
                x.DataType == y.DataType;
        }

        static int ToInt(bool value) => value ? 0 : 1;

        public override int Compare(Column x, Column y)
        {
            if (object.Equals(x, y)) { return 0; }
            if (x == null || y == null) { return 1; }

            var xNumeric = x as NumericColumn;
            var yNumeric = y as NumericColumn;
            if (xNumeric != null && yNumeric != null) { return ToInt(Equals(xNumeric, yNumeric)); }

            var xDateTime = x as DateTimeColumn;
            var yDateTime = y as DateTimeColumn;
            if (xDateTime != null && yDateTime != null) { return ToInt(Equals(xDateTime, yDateTime)); }

            var xString = x as StringColumn;
            var yString = y as StringColumn;
            if (xString != null && yString != null) { return ToInt(Equals(xString, yString)); }

            throw new NotSupportedException("Column type is not supported.");
        }
    }
}