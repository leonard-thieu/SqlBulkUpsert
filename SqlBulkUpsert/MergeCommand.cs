using System;
using System.Linq;
using System.Text;

namespace SqlBulkUpsert
{
    sealed class MergeCommand
    {
        public MergeCommand(
            string tableSource,
            SqlTableSchema targetTableSchema,
            bool updateWhenMatched)
        {
            this.targetTableSchema = targetTableSchema ?? throw new ArgumentNullException(nameof(targetTableSchema));
            this.tableSource = tableSource ?? throw new ArgumentNullException(nameof(tableSource));
            this.updateWhenMatched = updateWhenMatched;
        }

        readonly SqlTableSchema targetTableSchema;
        readonly string tableSource;
        readonly bool updateWhenMatched;

        public override string ToString()
        {
            var targetTable = targetTableSchema.TableName;
            var mergeSearchCondition = GetMergeSearchCondition();
            var setClause = GetSetClause();
            var columnList = GetValuesList();
            var valuesList = GetValuesList();

            var sb = new StringBuilder();

            sb.AppendLine($"MERGE INTO [{targetTable}] AS [Target]");
            sb.AppendLine($"USING [{tableSource}] AS [Source]");
            sb.AppendLine($"    ON ({mergeSearchCondition})");

            if (updateWhenMatched)
            {
                sb.AppendLine($"WHEN MATCHED");
                sb.AppendLine($"    THEN");
                sb.AppendLine($"        UPDATE");
                sb.AppendLine($"        SET {setClause}");
            }

            sb.AppendLine($"WHEN NOT MATCHED");
            sb.AppendLine($"    THEN");
            sb.AppendLine($"        INSERT ({columnList})");
            sb.AppendLine($"        VALUES ({valuesList});");

            return sb.ToString();
        }

        string GetMergeSearchCondition()
        {
            var columns = from c in targetTableSchema.PrimaryKeyColumns
                          let column = c.ToSelectListString()
                          select $"[Target].{column} = [Source].{column}";

            return string.Join(" AND ", columns);
        }

        string GetSetClause()
        {
            // Exclude primary key and identity columns
            var columns = from c in targetTableSchema.Columns.Except(targetTableSchema.PrimaryKeyColumns)
                          let column = c.ToSelectListString()
                          select $"[Target].{column} = [Source].{column}";

            return string.Join(",\r\n            ", columns);
        }

        string GetValuesList() => targetTableSchema.Columns.ToSelectListString();
    }
}