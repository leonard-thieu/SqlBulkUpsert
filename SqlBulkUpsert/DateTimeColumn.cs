namespace SqlBulkUpsert
{
    sealed class DateTimeColumn : Column
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
                    return $"{DataType}({Precision})";
            }

            return base.ToFullDataTypeString();
        }
    }
}