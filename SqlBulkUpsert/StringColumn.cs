using System;

namespace SqlBulkUpsert
{
    internal sealed class StringColumn : ColumnBase
    {
        public StringColumn(string name, int ordinalPosition, bool isNullable, string dataType, int? charLength = null, int? byteLength = null) :
            base(name, ordinalPosition, isNullable, dataType)
        {
            CharLength = charLength;
            ByteLength = byteLength;
        }

        public int? CharLength { get; }
        public int? ByteLength { get; }

        protected override string ToFullDataTypeString()
        {
            switch (DataType)
            {
                case "char":
                case "varchar":
                case "nchar":
                case "nvarchar":
                    return $"{DataType}({HandleMax(CharLength)})";

                case "binary":
                case "varbinary":
                    return $"{DataType}({HandleMax(ByteLength)})";

                default:
                    return base.ToFullDataTypeString();
            }

            string HandleMax(int? value)
            {
                return value == -1 ?
                    "max" :
                    Convert.ToString(value);
            }
        }
    }
}