namespace SqlBulkUpsert
{
    internal sealed class DateTimeColumn : ColumnBase
    {
        public DateTimeColumn(string name, int ordinalPosition, bool isNullable, string dataType, int? precision) :
            base(name, ordinalPosition, isNullable, dataType)
        {
            Precision = precision;
        }

        public int? Precision { get; }

        protected override string ToFullDataTypeString()
        {
            switch (DataType)
            {
                case "datetimeoffset":
                case "datetime2":
                case "time":
                    if (Precision != null) { return $"{DataType}({Precision})"; }
                    break;

                default:
                    break;
            }

            return base.ToFullDataTypeString();
        }
    }
}