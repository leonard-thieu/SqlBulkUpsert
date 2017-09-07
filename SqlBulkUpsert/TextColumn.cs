using System;
using System.Data;
using System.Globalization;

namespace SqlBulkUpsert
{
    sealed class TextColumn : Column
    {
        public int? CharLength { get; set; }
        public int? ByteLength { get; set; }

        public override bool Equals(Column other)
        {
            var textOther = other as TextColumn;
            if (textOther == null)
                return false;

            return
                 base.Equals(other) &&
                 CharLength == textOther.CharLength &&
                 ByteLength == textOther.ByteLength;
        }

        public override Column Clone()
        {
            return CopyTo(new TextColumn());
        }

        public override Column CopyTo(Column column)
        {
            var textColumn = (TextColumn)column;
            textColumn.CharLength = CharLength;
            textColumn.ByteLength = ByteLength;
            return base.CopyTo(textColumn);
        }

        public override string ToFullDataTypeString()
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

        static string HandleMax(int? val)
        {
            if (!val.HasValue)
                throw new InvalidOperationException("Expected column length.");
            var value = val.Value;

            return value == -1 ?
                "max" : value.ToString(CultureInfo.InvariantCulture);
        }

        protected override void Populate(IDataReader sqlDataReader)
        {
            base.Populate(sqlDataReader);

            CharLength = sqlDataReader.GetValue<int?>("CHARACTER_MAXIMUM_LENGTH");
            ByteLength = sqlDataReader.GetValue<int?>("CHARACTER_OCTET_LENGTH");
        }
    }
}