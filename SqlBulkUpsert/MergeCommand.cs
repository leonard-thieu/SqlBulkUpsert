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
            bool updateOnMatch,
            string sourceSearchCondition)
        {
            this.targetTableSchema = targetTableSchema ?? throw new ArgumentNullException(nameof(targetTableSchema));
            this.tableSource = tableSource ?? throw new ArgumentNullException(nameof(tableSource));
            this.updateOnMatch = updateOnMatch;
            this.sourceSearchCondition = sourceSearchCondition;
        }

        readonly SqlTableSchema targetTableSchema;
        readonly string tableSource;
        readonly bool updateOnMatch;
        readonly string sourceSearchCondition;

        public override string ToString()
        {
            var targetTable = targetTableSchema.TableName;
            var tableSource = this.tableSource;
            var mergeSearchCondition = GetMergeSearchCondition();
            var sourceSearchCondition = GetSearchCondition(this.sourceSearchCondition);
            var setClause = GetSetClause();
            var columnList = GetValuesList();
            var valuesList = GetValuesList();

            var sb = new StringBuilder();

            sb.AppendFormatLine("MERGE INTO [{0}] AS [Target]", targetTable);
            sb.AppendFormatLine("USING [{0}] AS [Source]", tableSource);
            sb.AppendFormatLine("    ON ({0})", mergeSearchCondition);

            if (updateOnMatch)
            {
                sb.AppendFormatLine("WHEN MATCHED");
                sb.AppendFormatLine("    THEN");
                sb.AppendFormatLine("        UPDATE");
                sb.AppendFormatLine("        SET {0}", setClause);
            }

            sb.AppendFormatLine("WHEN NOT MATCHED");
            sb.AppendFormatLine("    THEN");
            sb.AppendFormatLine("        INSERT ({0})", columnList);

            if (this.sourceSearchCondition == null)
            {
                sb.AppendFormatLine("        VALUES ({0});", valuesList);
            }
            else
            {
                sb.AppendFormatLine("        VALUES ({0})", valuesList);
                sb.AppendFormatLine("WHEN NOT MATCHED BY SOURCE {0}", sourceSearchCondition);
                sb.AppendFormatLine("    THEN");
                sb.AppendFormatLine("        DELETE;");
            }

            return sb.ToString();
        }

        string GetSearchCondition(string searchCondition)
        {
            if (searchCondition == null)
            {
                return string.Empty;
            }
            else
            {
                return $"AND {searchCondition}";
            }
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