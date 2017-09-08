namespace SqlBulkUpsert
{
    sealed class NumericColumn : ColumnBase
    {
        public NumericColumn(string name, int ordinalPosition, bool isNullable, string dataType, int? precision = null, int? radix = null, int? scale = null) :
            base(name, ordinalPosition, isNullable, dataType)
        {
            Precision = precision;
            Radix = radix;
            Scale = scale;
        }

        public int? Precision { get; }
        public int? Radix { get; }
        public int? Scale { get; }

        protected override string ToFullDataTypeString()
        {
            switch (DataType)
            {
                case "numeric":
                case "decimal":
                    return $"{DataType}({Precision}, {Scale})";

                case "float":
                case "real":
                    return $"{DataType}({Radix})";

                default:
                    return base.ToFullDataTypeString();
            }
        }
    }
}