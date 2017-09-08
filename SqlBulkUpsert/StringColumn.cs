using System;

namespace SqlBulkUpsert
{
    sealed class StringColumn : Column
    {
        static string HandleMax(int? val)
        {
            if (!val.HasValue)
                throw new InvalidOperationException("Expected column length.");

            var value = val.Value;

            return value == -1 ?
                "max" :
                Convert.ToString(val);
        }

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
        }
    }
}