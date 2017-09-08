using System;
using System.Data;

namespace SqlBulkUpsert
{
    static class ColumnFactory
    {
        public static ColumnBase CreateFromReader(IDataReader sqlDataReader)
        {
            if (sqlDataReader == null)
                throw new ArgumentNullException(nameof(sqlDataReader));

            var name = (string)sqlDataReader["COLUMN_NAME"];
            var ordinalPosition = (int)sqlDataReader["ORDINAL_POSITION"];
            var isNullable = ((string)sqlDataReader["IS_NULLABLE"]) == "YES";
            var dataType = (string)sqlDataReader["DATA_TYPE"];

            switch (dataType)
            {
                // Exact numerics
                case "bigint":
                case "numeric":
                case "bit":
                case "smallint":
                case "decimal":
                case "smallmoney":
                case "int":
                case "tinyint":
                case "money":
                // Approximate numerics
                case "float":
                case "real":
                    {
                        var precision = sqlDataReader.GetValue<byte?>("NUMERIC_PRECISION");
                        var radix = sqlDataReader.GetValue<short?>("NUMERIC_PRECISION_RADIX");
                        var scale = sqlDataReader.GetValue<int?>("NUMERIC_SCALE");

                        return new NumericColumn(name, ordinalPosition, isNullable, dataType, precision, radix, scale);
                    }

                // Date and time
                case "date":
                case "datetimeoffset":
                case "datetime2":
                case "smalldatetime":
                case "datetime":
                case "time":
                    {
                        var precision = sqlDataReader.GetValue<short?>("DATETIME_PRECISION");

                        return new DateTimeColumn(name, ordinalPosition, isNullable, dataType, precision);
                    }

                // Character strings
                case "char":
                case "varchar":
                case "text":
                // Unicode character strings
                case "nchar":
                case "nvarchar":
                case "ntext":
                // Binary strings
                case "binary":
                case "varbinary":
                case "image":
                    {
                        var charLength = sqlDataReader.GetValue<int?>("CHARACTER_MAXIMUM_LENGTH");
                        var byteLength = sqlDataReader.GetValue<int?>("CHARACTER_OCTET_LENGTH");

                        return new StringColumn(name, ordinalPosition, isNullable, dataType, charLength, byteLength);
                    }

                default:
                    throw new NotSupportedException($"The SQL data type '{dataType}' is not supported.");
            }
        }
    }
}
