using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace SqlBulkUpsert
{
    sealed class TypedDataReader<T> : IDataReader
    {
        public TypedDataReader(ColumnMappings<T> columnMappings, IEnumerable<T> items)
        {
            if (columnMappings == null)
                throw new ArgumentNullException(nameof(columnMappings));
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            mappingLookup = columnMappings.Keys
                .Select((s, i) => new { Key = s, Value = i })
                .ToDictionary(x => x.Key, x => x.Value);
            mappingFuncs = columnMappings.Values.ToList();
            this.items = items.GetEnumerator();
        }

        readonly Dictionary<string, int> mappingLookup;
        readonly IList<Func<T, object>> mappingFuncs;
        readonly IEnumerator<T> items;

        public object GetValue(int i) => mappingFuncs[i](items.Current);

        public int GetOrdinal(string name) => mappingLookup[name];

        public int FieldCount => mappingFuncs.Count;

        public bool Read() => items.MoveNext();

        #region Not used by SqlBulkCopy (satisfying interface only)

        [ExcludeFromCodeCoverage]
        public string GetName(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public string GetDataTypeName(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public Type GetFieldType(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public int GetValues(object[] values) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public bool GetBoolean(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public byte GetByte(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public long GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public char GetChar(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public long GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public Guid GetGuid(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public short GetInt16(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public int GetInt32(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public long GetInt64(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public float GetFloat(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public double GetDouble(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public string GetString(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public decimal GetDecimal(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public DateTime GetDateTime(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public IDataReader GetData(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public bool IsDBNull(int i) => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        object IDataRecord.this[int i] => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        object IDataRecord.this[string name] => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public void Close() => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public DataTable GetSchemaTable() => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public bool NextResult() => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public int Depth => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public bool IsClosed => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public int RecordsAffected => throw new NotImplementedException();

        [ExcludeFromCodeCoverage]
        public void Dispose() { }

        #endregion
    }
}