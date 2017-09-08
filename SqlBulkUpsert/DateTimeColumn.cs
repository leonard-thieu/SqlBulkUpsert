namespace SqlBulkUpsert
{
    sealed class DateTimeColumn : ColumnBase
    {
        public DateTimeColumn(string name, int ordinalPosition, bool isNullable, string dataType, int? precision) :
            base(name, ordinalPosition, isNullable, dataType)
        {
            Precision = precision;
        }

        public int? Precision { get; }

        protected override string ToFullDataTypeString()
        {
            if (Precision != null)
            {
                switch (DataType)
                {
                    case "datetimeoffset":
                    case "datetime2":
                    case "time":
                        return $"{DataType}({Precision})";

                    default:
                        break;
                }
            }

            return base.ToFullDataTypeString();
        }
    }
}