namespace SqlBulkUpsert
{
    abstract class ColumnBase
    {
        protected ColumnBase(string name, int ordinalPosition, bool isNullable, string dataType)
        {
            Name = name;
            OrdinalPosition = ordinalPosition;
            IsNullable = isNullable;
            DataType = dataType;
        }

        public string Name { get; }
        public int OrdinalPosition { get; }
        public bool IsNullable { get; }
        public string DataType { get; }

        public string ToSelectListString() => $"[{Name}]";

        public string ToColumnDefinitionString()
        {
            return IsNullable ?
                $"{ToSelectListString()} {ToFullDataTypeString()} NULL" :
                $"{ToSelectListString()} {ToFullDataTypeString()} NOT NULL";
        }

        protected virtual string ToFullDataTypeString() => DataType;
    }
}