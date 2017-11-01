using System;
using System.Collections.Generic;

namespace SqlBulkUpsert.Tests
{
    internal sealed class ColumnComparer : EqualityComparer<ColumnBase>
    {
        private static bool Equals(NumericColumn x, NumericColumn y)
        {
            return
                x.Precision == y.Precision &&
                x.Radix == y.Radix &&
                x.Scale == y.Scale &&
                EqualImpl((ColumnBase)x, (ColumnBase)y);
        }

        private static bool Equals(DateTimeColumn x, DateTimeColumn y)
        {
            return
                x.Precision == y.Precision &&
                EqualImpl((ColumnBase)x, (ColumnBase)y);
        }

        private static bool Equals(StringColumn x, StringColumn y)
        {
            return
                x.CharLength == y.CharLength &&
                x.ByteLength == y.ByteLength &&
                EqualImpl((ColumnBase)x, (ColumnBase)y);
        }

        private static bool EqualImpl(ColumnBase x, ColumnBase y)
        {
            return
                x.Name == y.Name &&
                x.OrdinalPosition == y.OrdinalPosition &&
                x.IsNullable == y.IsNullable &&
                x.DataType == y.DataType;
        }

        private static int ToInt(bool value) => value ? 0 : 1;

        public override bool Equals(ColumnBase x, ColumnBase y)
        {
            if (object.Equals(x, y)) { return true; }
            if (x == null || y == null) { return false; }

            var xNumeric = x as NumericColumn;
            var yNumeric = y as NumericColumn;
            if (xNumeric != null && yNumeric != null) { return Equals(xNumeric, yNumeric); }

            var xDateTime = x as DateTimeColumn;
            var yDateTime = y as DateTimeColumn;
            if (xDateTime != null && yDateTime != null) { return Equals(xDateTime, yDateTime); }

            var xString = x as StringColumn;
            var yString = y as StringColumn;
            if (xString != null && yString != null) { return Equals(xString, yString); }

            throw new NotSupportedException("Column type is not supported.");
        }

        public override int GetHashCode(ColumnBase obj) => 0;
    }
}