using System.Globalization;
using LogMiner.Log;
using LogMiner.Schema;
using Microsoft.Data.SqlClient;

namespace LogMiner.SqlServer;

/// <summary>Catalog row changes found in a database's log, with the outcome of the transactions that made them.</summary>
public sealed record CatalogLog(IReadOnlyList<CatalogChange> Changes, IReadOnlyDictionary<ulong, TransactionOutcome> Outcomes);

/// <summary>Allocation unit and rowset of a system base table.</summary>
public sealed record SystemTable(CatalogTable Table, int ObjectId, long RowsetId, long AllocUnitId);

/// <summary>
/// Reads the catalog's own row changes (system base tables) from the log. Only used when layouts are (re)loaded; the scan covers the
/// log that is still to be decoded, which is a few blocks while the stream is live.
/// </summary>
public static class CatalogLogReader
{
    private const string Clamp = """
        DECLARE @min nvarchar(46) = (SELECT log_min_lsn FROM sys.dm_db_log_stats(@dbid));
        IF @min IS NULL THROW 50001, 'database is not readable (offline or no log stats)', 1;
        IF @start IS NULL OR @start < @min SET @start = @min;

        """;

    private const string ChangesQuery = Clamp + """
        SELECT [Current LSN], Operation, [Transaction ID], CONVERT(int, [Flag Bits]), AllocUnitId, ISNULL([Offset in Row], 0),
               [RowLog Contents 0], [RowLog Contents 1], [RowLog Contents 2], [Previous LSN], CASE WHEN Operation = N'LOP_MODIFY_COLUMNS' THEN [RowLog Contents 4] END
        FROM sys.fn_full_dblog('0x' + @start, NULL, @dbid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        WHERE Operation IN (N'LOP_INSERT_ROWS', N'LOP_DELETE_ROWS', N'LOP_MODIFY_ROW', N'LOP_MODIFY_COLUMNS')
          AND Context IN (N'LCX_CLUSTERED', N'LCX_MARK_AS_GHOST')
          AND AllocUnitId IN (@au0, @au1, @au2, @au3, @au4)
        OPTION (MAXDOP 1) /*logminer-catalog*/;
        """;

    // [Xact Type] 3 = system transaction (e.g. a page split moving catalog rows: not a change)
    private const string OutcomesQuery = Clamp + """
        SELECT [Transaction ID], Operation, [Current LSN], ISNULL([Xact Type], 0),
               CASE WHEN [Transaction Begin] COLLATE Latin1_General_BIN2 < @min COLLATE Latin1_General_BIN2 THEN 1 ELSE 0 END
        FROM sys.fn_full_dblog('0x' + @start, NULL, @dbid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        WHERE Operation IN (N'LOP_BEGIN_XACT', N'LOP_COMMIT_XACT', N'LOP_ABORT_XACT')
          AND [Transaction ID] IN (SELECT value COLLATE DATABASE_DEFAULT FROM STRING_SPLIT(@xids, N','))
        OPTION (MAXDOP 1) /*logminer-catalog*/;
        """;

    public static async Task<CatalogLog> ReadAsync(SqlConnection conn, int databaseId, Lsn? from, IReadOnlyList<SystemTable> tables, CancellationToken ct)
    {
        var byAllocUnit = tables.ToDictionary(t => t.AllocUnitId, t => t.Table);
        var changes = new List<CatalogChange>();
        await using (var cmd = Command(conn, ChangesQuery, databaseId, from))
        {
            for (var i = 0; i < 5; i++)
                cmd.Parameters.Add(new SqlParameter("@au" + i.ToString(CultureInfo.InvariantCulture), System.Data.SqlDbType.BigInt) { Value = i < tables.Count ? tables[i].AllocUnitId : 0L });
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
                changes.Add(new CatalogChange
                {
                    Lsn = Lsn.Parse(r.GetString(0)),
                    Operation = LogRecord.ParseOperation(r.GetString(1)),
                    TransactionId = LogRecord.ParseTransactionId(r.GetString(2)),
                    Compensation = ((LogRecordBits)r.GetInt32(3) & LogRecordBits.Compensation) != 0,
                    Table = byAllocUnit[r.GetInt64(4)],
                    Offset = r.GetInt16(5),
                    Row0 = r.IsDBNull(6) ? null : r.GetFieldValue<byte[]>(6),
                    Row1 = r.IsDBNull(7) ? null : r.GetFieldValue<byte[]>(7),
                    Key = r.IsDBNull(8) ? null : r.GetFieldValue<byte[]>(8),
                    UndoneAfter = r.IsDBNull(9) ? default : Lsn.Parse(r.GetString(9)),
                    Data = r.IsDBNull(10) ? null : r.GetFieldValue<byte[]>(10),
                });
        }
        var outcomes = new Dictionary<ulong, TransactionOutcome>();
        if (changes.Count == 0) return new CatalogLog(changes, outcomes);

        var xids = changes.Select(c => c.TransactionId).ToHashSet();
        var begun = await ReadOutcomesAsync(conn, databaseId, from, xids, outcomes, ct);
        // a transaction that began before the scanned range: its type (user or system) is in its begin record, further back
        if (from is not null && !begun.SetEquals(xids)) begun = await ReadOutcomesAsync(conn, databaseId, null, xids, outcomes, ct);
        foreach (var x in xids.Where(x => !begun.Contains(x))) outcomes.Remove(x); // unknown: the history builder refuses to guess
        return new CatalogLog(changes, outcomes);
    }

    private static async Task<HashSet<ulong>> ReadOutcomesAsync(SqlConnection conn, int databaseId, Lsn? from, HashSet<ulong> xids,
                                                                Dictionary<ulong, TransactionOutcome> outcomes, CancellationToken ct)
    {
        var begun = new HashSet<ulong>();
        outcomes.Clear();
        await using var cmd = Command(conn, OutcomesQuery, databaseId, from);
        cmd.Parameters.Add(new SqlParameter("@xids", System.Data.SqlDbType.NVarChar, -1) { Value = string.Join(',', xids.Select(LogRecord.FormatTransactionId)) });
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct))
        {
            var xid = LogRecord.ParseTransactionId(r.GetString(0));
            var o = outcomes.GetValueOrDefault(xid);
            switch (LogRecord.ParseOperation(r.GetString(1)))
            {
                case LogOperation.BeginXact: begun.Add(xid); outcomes[xid] = o with { IsSystem = r.GetInt32(3) == 3 }; break;
                case LogOperation.CommitXact: outcomes[xid] = o with { Commit = Lsn.Parse(r.GetString(2)) }; break;
                case LogOperation.AbortXact: outcomes[xid] = o with { Aborted = true }; break;
            }
            // It began before the oldest record of the log, so its type is not there any more. A system transaction (a page split) is short and
            // ends before anything else can; one that has outlived a truncation is a user's.
            if (LogRecord.ParseOperation(r.GetString(1)) is LogOperation.CommitXact or LogOperation.AbortXact && r.GetInt32(4) == 1) begun.Add(xid);
        }
        return begun;
    }

    private static SqlCommand Command(SqlConnection conn, string sql, int databaseId, Lsn? from)
    {
        var cmd = new SqlCommand(sql, conn) { CommandTimeout = 0 }; // a long backlog is one long scan; cancellation ends it
        cmd.Parameters.Add(new SqlParameter("@dbid", System.Data.SqlDbType.Int) { Value = databaseId });
        cmd.Parameters.Add(new SqlParameter("@start", System.Data.SqlDbType.NVarChar, 46) { Value = from is { } s ? s.ToString() : DBNull.Value });
        return cmd;
    }
}
