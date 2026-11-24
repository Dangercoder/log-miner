using System.Data;
using System.Data.SqlTypes;
using System.Runtime.InteropServices;
using System.Text;
using LogMiner.Capture;
using LogMiner.Rows;
using LogMiner.Schema;
using Microsoft.Data.SqlClient;

namespace LogMiner.SqlServer;

/// <summary>
/// Reads snapshot chunks: <c>log end (L) → TOP (n) rows in key order WITH (READCOMMITTEDLOCK) → log end (H)</c>, one round trip.
/// Locking read committed is forced even where RCSI is on (see <see cref="TableSnapshot"/> for why). A short lock timeout keeps
/// a chunk from waiting long behind writers; the chunk is retried on the next pump. Values are converted to exactly the CLR
/// types the log decoder produces (text is read as bytes and decoded like the log's), so snapshot rows and stream rows are indistinguishable to
/// consumers. A table some of whose rows or values the login does not see (row-level security, masking) is refused: the stream has them all.
/// </summary>
public sealed class SqlSnapshotSource(string connectionString, int lockTimeoutMs = 5000) : ISnapshotSource
{
    private sealed record Column(string? Collation, bool IsClr);

    /// <summary>What the layout does not say: collations (to send text keys in the right code page), CLR types, and whether all of it is visible.</summary>
    private static async Task<Dictionary<string, Column>> ColumnsAsync(SqlConnection conn, TableLayout table, CancellationToken ct)
    {
        await using var cmd = new SqlCommand($"EXEC {SchemaLoader.Quote(table.Database)}.sys.sp_executesql @stmt, N'@id int', @id", conn) { CommandTimeout = 60 };
        cmd.Parameters.Add(new SqlParameter("@stmt", SqlDbType.NVarChar, -1) { Value = """
            SELECT c.name, c.collation_name, CONVERT(bit, CASE WHEN c.system_type_id = 240 THEN 1 ELSE 0 END),
                   CONVERT(bit, CASE WHEN c.is_masked = 1 AND ISNULL(HAS_PERMS_BY_NAME(DB_NAME(), N'DATABASE', N'UNMASK'), 0) = 0 THEN 1 ELSE 0 END)
            FROM sys.columns c WHERE c.object_id = @id;
            SELECT COUNT(*) FROM sys.security_predicates p JOIN sys.security_policies s ON s.object_id = p.object_id WHERE p.target_object_id = @id AND p.predicate_type = 0 AND s.is_enabled = 1;
            """ });
        cmd.Parameters.AddWithValue("@id", table.ObjectId);
        var columns = new Dictionary<string, Column>(StringComparer.Ordinal);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            if (r.GetBoolean(3)) throw new InvalidOperationException($"{table}: column {r.GetString(0)} is masked and the login may not see it (grant UNMASK)");
            columns[r.GetString(0)] = new Column(r.IsDBNull(1) ? null : r.GetString(1), r.GetBoolean(2));
        }
        await r.NextResultAsync(ct);
        await r.ReadAsync(ct);
        if (r.GetInt32(0) > 0) throw new InvalidOperationException($"{table}: a row-level security filter applies; a historical load could silently lack rows (exempt the login in the predicate and disable the policy for the load)");
        return columns;
    }

    public async Task<SnapshotChunk> ReadChunkAsync(TableLayout table, object?[]? afterKey, int chunkSize, CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        var columns = await ColumnsAsync(conn, table, ct);
        var name = $"{SchemaLoader.Quote(table.Database)}.{SchemaLoader.Quote(table.Schema)}.{SchemaLoader.Quote(table.Table)}";
        var key = table.Key;
        var select = string.Join(", ", table.Visible.Select(c => SelectExpression(c, columns.GetValueOrDefault(c.Name))));
        var order = string.Join(", ", key.Select(k => SchemaLoader.Quote(k.Name)));
        var where = afterKey is null ? "" : "WHERE " + KeysetPredicate(key, afterKey, columns);
        var sql = $"""
            SET LOCK_TIMEOUT {lockTimeoutMs};
            SELECT log_end_lsn FROM sys.dm_db_log_stats(DB_ID(@db));
            SELECT TOP (@n) {select} FROM {name} WITH (READCOMMITTEDLOCK) {where} ORDER BY {order} OPTION (MAXDOP 1) /*logminer-snapshot*/;
            SELECT log_end_lsn FROM sys.dm_db_log_stats(DB_ID(@db));
            """;
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
        cmd.Parameters.AddWithValue("@db", table.Database);
        cmd.Parameters.AddWithValue("@n", chunkSize);
        if (afterKey is not null)
            for (var i = 0; i < key.Count; i++) if (afterKey[i] is { } value) cmd.Parameters.Add(Param($"@k{i}", key[i], value));

        var rows = new List<RowImage>();
        Lsn low, high;
        await using (var r = await cmd.ExecuteReaderAsync(ct))
        {
            await r.ReadAsync(ct);
            low = Lsn.Parse(r.GetString(0));
            await r.NextResultAsync(ct);
            while (await r.ReadAsync(ct))
            {
                var values = new object?[table.Visible.Count];
                for (var i = 0; i < values.Length; i++) values[i] = Convert(r, i, table.Visible[i]);
                rows.Add(new RowImage(table, values));
            }
            await r.NextResultAsync(ct);
            await r.ReadAsync(ct);
            high = Lsn.Parse(r.GetString(0));
        }
        object?[]? lastKey = rows.Count == 0 ? null : key.Select(k => rows[^1].Values[table.IndexOf(k.ColumnId)]).ToArray();
        return new SnapshotChunk { Table = table, Low = low, High = high, Rows = rows, LastKey = lastKey };
    }

    /// <summary>(k1 > @k0) OR (k1 = @k0 AND k2 > @k1) OR ... - keyset pagination on a composite key. NULL sorts first (a unique clustered index
    /// may have nullable columns).</summary>
    private static string KeysetPredicate(IReadOnlyList<ColumnLayout> key, object?[] after, Dictionary<string, Column> columns)
    {
        var sb = new StringBuilder();
        for (var i = 0; i < key.Count; i++)
        {
            if (i > 0) sb.Append(" OR ");
            sb.Append('(');
            for (var j = 0; j < i; j++) sb.Append(SchemaLoader.Quote(key[j].Name)).Append(after[j] is null ? " IS NULL" : " = " + Value(j)).Append(" AND ");
            sb.Append(SchemaLoader.Quote(key[i].Name)).Append(after[i] is null ? " IS NOT NULL" : " > " + Value(i));
            sb.Append(')');
        }
        return sb.ToString();

        string Value(int i) => key[i].Type switch
        {
            // SqlClient would send char and varchar in the connection's code page, whatever the column's is, and replace half surrogate pairs
            SqlType.Char or SqlType.VarChar => $"CONVERT(varchar({Length(key[i], 1)}), @k{i} COLLATE {columns[key[i].Name].Collation})",
            SqlType.NChar or SqlType.NVarChar => $"CONVERT(nvarchar({Length(key[i], 2)}), @k{i}) COLLATE {columns[key[i].Name].Collation}",
            _ => "@k" + i.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
        static int Length(ColumnLayout c, int unit) => Math.Max(1, c.MaxLength / unit);
    }

    private static string SelectExpression(ColumnLayout c, Column? column)
    {
        var q = SchemaLoader.Quote(c.Name);
        if (column is { IsClr: true }) return $"CAST({q} AS varbinary(max))"; // no CLR types needed
        return c.Type switch
        {
            SqlType.HierarchyId or SqlType.Geometry or SqlType.Geography => $"CAST({q} AS varbinary(max))",
            SqlType.Char or SqlType.VarChar or SqlType.NChar or SqlType.NVarChar => $"CAST({q} AS varbinary(max))", // as stored; decoded like the log's
            SqlType.Xml => $"CAST({q} AS nvarchar(max))",
            SqlType.Text => $"CAST(CAST({q} AS varchar(max)) AS varbinary(max))",
            SqlType.NText => $"CAST(CAST({q} AS nvarchar(max)) AS varbinary(max))",
            SqlType.Image => $"CAST({q} AS varbinary(max))",
            SqlType.SqlVariant => $"CASE WHEN {q} IS NULL THEN NULL ELSE 1 END",
            _ => q,
        };
    }

    private static SqlParameter Param(string name, ColumnLayout c, object value)
    {
        switch (c.Type)
        {
            case SqlType.Char or SqlType.VarChar:
                return new SqlParameter(name, SqlDbType.NVarChar, -1) { Value = value as string ?? throw new InvalidOperationException($"key column {c.Name} holds bytes that are not text in its code page") };
            case SqlType.NChar or SqlType.NVarChar:
                return new SqlParameter(name, SqlDbType.VarBinary, -1) { Value = MemoryMarshal.AsBytes(((string)value).AsSpan()).ToArray() };
        }
        var p = new SqlParameter(name, DbType(c)) { Value = value switch { DateOnly d => d.ToDateTime(TimeOnly.MinValue), string s when c.Type == SqlType.Decimal => SqlDecimal.Parse(s), _ => value } };
        if (c.Type is SqlType.Binary or SqlType.VarBinary) p.Size = c.MaxLength < 0 ? -1 : c.MaxLength;
        if (c.Type == SqlType.Decimal) { p.Precision = c.Precision; p.Scale = c.Scale; }
        if (c.Type is SqlType.Time or SqlType.DateTime2 or SqlType.DateTimeOffset) p.Scale = c.Scale;
        return p;
    }

    private static SqlDbType DbType(ColumnLayout c) => c.Type switch
    {
        SqlType.TinyInt => SqlDbType.TinyInt, SqlType.SmallInt => SqlDbType.SmallInt, SqlType.Int => SqlDbType.Int, SqlType.BigInt => SqlDbType.BigInt,
        SqlType.Bit => SqlDbType.Bit, SqlType.Decimal => SqlDbType.Decimal, SqlType.Money => SqlDbType.Money, SqlType.SmallMoney => SqlDbType.SmallMoney,
        SqlType.Float => SqlDbType.Float, SqlType.Real => SqlDbType.Real, SqlType.Date => SqlDbType.Date, SqlType.Time => SqlDbType.Time,
        SqlType.DateTime2 => SqlDbType.DateTime2, SqlType.DateTimeOffset => SqlDbType.DateTimeOffset, SqlType.DateTime => SqlDbType.DateTime,
        SqlType.SmallDateTime => SqlDbType.SmallDateTime, SqlType.Char => SqlDbType.Char, SqlType.VarChar => SqlDbType.VarChar,
        SqlType.NChar => SqlDbType.NChar, SqlType.NVarChar => SqlDbType.NVarChar, SqlType.Binary => SqlDbType.Binary,
        SqlType.VarBinary => SqlDbType.VarBinary, SqlType.UniqueIdentifier => SqlDbType.UniqueIdentifier, SqlType.RowVersion => SqlDbType.Timestamp,
        _ => SqlDbType.VarBinary,
    };

    /// <summary>SqlClient value → the CLR type <see cref="ValueDecoder"/> produces for the same column.</summary>
    private static object? Convert(SqlDataReader r, int i, ColumnLayout c)
    {
        if (r.IsDBNull(i)) return null;
        switch (c.Type)
        {
            case SqlType.SqlVariant: return Unavailable.Value;
            case SqlType.Char or SqlType.VarChar or SqlType.Text: return ValueDecoder.DecodeText(r.GetFieldValue<byte[]>(i), c.CodePage);
            case SqlType.NChar or SqlType.NVarChar or SqlType.NText: return new string(MemoryMarshal.Cast<byte, char>(r.GetFieldValue<byte[]>(i)));
            case SqlType.Date: return DateOnly.FromDateTime(r.GetDateTime(i));
            case SqlType.Decimal:
            {
                var d = r.GetSqlDecimal(i);
                if (c.Scale <= 28)
                {
                    try { return d.Value; } catch (OverflowException) { } // beyond System.Decimal: exact text, like the decoder
                }
                return d.ToString();
            }
            case SqlType.Money or SqlType.SmallMoney: return ValueDecoder.FromScaledInteger(decimal.ToInt64(r.GetDecimal(i) * 10000m), 4); // exact: money has 4 decimals; keep scale 4 like the decoder
            case SqlType.Real: return r.GetFloat(i);
            case SqlType.Float: return r.GetDouble(i);
            case SqlType.Time: return r.GetTimeSpan(i);
            case SqlType.DateTimeOffset: return r.GetDateTimeOffset(i);
            case SqlType.Bit: return r.GetBoolean(i);
            case SqlType.TinyInt: return r.GetByte(i);
            case SqlType.SmallInt: return r.GetInt16(i);
            case SqlType.Int: return r.GetInt32(i);
            case SqlType.BigInt: return r.GetInt64(i);
            case SqlType.UniqueIdentifier: return r.GetGuid(i);
            default:
                return r.GetValue(i);
        }
    }
}
