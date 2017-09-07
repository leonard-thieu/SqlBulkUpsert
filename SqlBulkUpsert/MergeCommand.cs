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
            bool updateOnMatch)
        {
            this.targetTableSchema = targetTableSchema ?? throw new ArgumentNullException(nameof(targetTableSchema));
            this.tableSource = tableSource ?? throw new ArgumentNullException(nameof(tableSource));
            this.updateOnMatch = updateOnMatch;
        }

        readonly SqlTableSchema targetTableSchema;
        readonly string tableSource;
        readonly bool updateOnMatch;

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

            if (updateOnMatch)
            {
                sb.AppendLine("WHEN MATCHED");
                sb.AppendLine("    THEN");
                sb.AppendLine("        UPDATE");
                sb.AppendLine($"        SET {setClause}");
            }

            sb.AppendLine("WHEN NOT MATCHED");
            sb.AppendLine("    THEN");
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
            var columns = from c in targetTableSchema.Columns
                          where c.CanBeUpdated
                          let column = c.ToSelectListString()
                          select $"[Target].{column} = [Source].{column}";

            return string.Join(",\r\n            ", columns);
        }

        string GetValuesList()
        {
            var columns = from c in targetTableSchema.Columns
                          where c.CanBeInserted
                          select c;

            return columns.ToSelectListString();
        }
    }
}