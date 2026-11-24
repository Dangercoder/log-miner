using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;
using LogMiner.Capture;
using LogMiner.Log;

namespace LogMiner.SqlServer;

/// <summary>
/// Reads a database's transaction log through the engine (sys.fn_full_dblog), so TDE-encrypted logs are decrypted
/// by SQL Server and the miner never holds key material.
/// <list type="bullet">
/// <item>One statement for every database (database id is a parameter): one cached plan for 1,200 databases.</item>
/// <item>The database is identified before and after the rows (<see cref="DatabaseIdentity"/>): the function reads whatever has the id now.</item>
/// <item>A @start that was truncated away is refused (<see cref="LogTruncatedException"/>), never moved silently: the function itself would
/// move it (experiments/31), so the first record must be the one that was asked for. @end is never passed - a non-existent @end raises
/// error 9003 (severity 20) which kills the session.</item>
/// <item>Only hardened log is read. The function also returns blocks that are still in memory, whose transactions a crash un-happens and
/// whose LSNs are then used again (experiments/29).</item>
/// <item>Filtered server-side to replicated/compensation records plus every commit/abort, plus one payload-free progress
/// marker per log block (slot 0001). Without markers a scan that matches nothing cannot advance, and every later read would
/// re-scan the same log - measured: 80-130 ms pumps that saturated the workers. Markers arrive as
/// <see cref="LogOperation.Other"/> with no flags or images and are ignored by the assembler.</item>
/// </list>
/// </summary>
public sealed class EngineLogReader(SqlConnection connection)
{
    // Within the block of the oldest record the log only holds what is not needed any more (released, or not replicated): start there.
    // (A caught-up reader's position is a block marker, the minimum is exact to the slot.)
    private const string Prologue = """
        DECLARE @min nvarchar(46), @end nvarchar(46), @why nvarchar(200);
        SELECT @min = log_min_lsn, @end = log_end_lsn FROM sys.dm_db_log_stats(@dbid);
        IF @min IS NULL OR @end IS NULL THROW 50001, 'database is not readable (offline or no log stats)', 1;
        IF LEFT(@start, 17) COLLATE Latin1_General_BIN2 < LEFT(@min, 17) COLLATE Latin1_General_BIN2 BEGIN SET @why = N'log starts at ' + @min; THROW 50002, @why, 1; END
        IF @start COLLATE Latin1_General_BIN2 >= @end COLLATE Latin1_General_BIN2 BEGIN SET @why = N'the log ends at ' + @end + N', before ' + @start + N' which was read earlier: the database was put back to an earlier state'; THROW 50004, @why, 1; END
        IF @start IS NULL OR @start COLLATE Latin1_General_BIN2 < @min COLLATE Latin1_General_BIN2 SET @start = @min;
        SELECT @start, CURRENT_TIMEZONE_ID();
        """;

    public const string ReplicatedQuery = DatabaseIdentity.Check + """

        """ + Prologue + """
        SELECT TOP (@max) [Current LSN], m.op, Context, [Transaction ID], CONVERT(int, [Flag Bits]) * m.keep, AllocUnitId, PartitionId,
               [Offset in Row], [Modify Size], [Log Record Length],
               CASE WHEN m.keep = 1 THEN [RowLog Contents 0] END, CASE WHEN m.keep = 1 THEN [RowLog Contents 1] END, NULL,
               [Transaction Begin], [End Time],
               CASE WHEN CONVERT(int, [Flag Bits]) & 1 = 1 THEN [Previous LSN] END, [Replicated Records]
        FROM sys.fn_full_dblog('0x' + @start, NULL, @dbid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        CROSS APPLY (SELECT CASE WHEN (CONVERT(int, [Flag Bits]) & 5) <> 0 OR Operation IN (N'LOP_COMMIT_XACT', N'LOP_ABORT_XACT') THEN 1 ELSE 0 END) k(keep)
        CROSS APPLY (SELECT k.keep, CASE WHEN k.keep = 1 THEN Operation ELSE N'LOP_PROGRESS' END) m(keep, op)
        WHERE (m.keep = 1 OR [Current LSN] LIKE N'%:0001' OR [Current LSN] = @start) -- one progress marker per log block: the scan position always advances
          AND [Current LSN] < @end
        OPTION (MAXDOP 1) /*logminer-read*/;

        """ + DatabaseIdentity.Check;

    /// <summary>No-CDC mode: every row operation on heaps/clustered indexes (client filters by allocation unit).</summary>
    public const string AllRowsQuery = DatabaseIdentity.Check + """

        """ + Prologue + """
        SELECT TOP (@max) [Current LSN], m.op, Context, [Transaction ID], CONVERT(int, [Flag Bits]) * m.keep, AllocUnitId, PartitionId,
               [Offset in Row], [Modify Size], [Log Record Length],
               CASE WHEN m.keep = 1 THEN [RowLog Contents 0] END, CASE WHEN m.keep = 1 THEN [RowLog Contents 1] END,
               CASE WHEN m.keep = 1 THEN [RowLog Contents 2] END, [Transaction Begin], [End Time],
               CASE WHEN CONVERT(int, [Flag Bits]) & 1 = 1 THEN [Previous LSN] END, [Replicated Records]
        FROM sys.fn_full_dblog('0x' + @start, NULL, @dbid, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        CROSS APPLY (SELECT CASE WHEN Operation IN (N'LOP_COMMIT_XACT', N'LOP_ABORT_XACT', N'LOP_REPL_COMMAND')
                                   OR (Context IN (N'LCX_HEAP', N'LCX_CLUSTERED', N'LCX_MARK_AS_GHOST')
                                       AND Operation IN (N'LOP_INSERT_ROWS', N'LOP_DELETE_ROWS', N'LOP_MODIFY_ROW', N'LOP_MODIFY_COLUMNS'))
                            THEN 1 ELSE 0 END) k(keep)
        CROSS APPLY (SELECT k.keep, CASE WHEN k.keep = 1 THEN Operation ELSE N'LOP_PROGRESS' END) m(keep, op)
        WHERE (m.keep = 1 OR [Current LSN] LIKE N'%:0001' OR [Current LSN] = @start) AND [Current LSN] < @end
        OPTION (MAXDOP 1) /*logminer-read*/;

        """ + DatabaseIdentity.Check;

    public SqlConnection Connection { get; } = connection;

    /// <summary>With the zone's rules at that time (a backlog may reach back over a clock change). In the hour that occurs twice, the second.</summary>
    internal static DateTime ToUtc(DateTime local, TimeZoneInfo zone) =>
        DateTime.SpecifyKind(local - (zone.IsInvalidTime(local) ? zone.BaseUtcOffset : zone.GetUtcOffset(local)), DateTimeKind.Utc);

    /// <summary>Streams records from <paramref name="start"/> (inclusive; null = start of the active log).</summary>
    public async IAsyncEnumerable<LogRecord> ReadAsync(DatabaseRef database, Lsn? start, int maxRows, CaptureScope scope,
                                                       [EnumeratorCancellation] CancellationToken ct)
    {
        await using var cmd = new SqlCommand(scope == CaptureScope.CdcTrackedTables ? ReplicatedQuery : AllRowsQuery, Connection) { CommandTimeout = 120 };
        DatabaseIdentity.AddParameters(cmd, database);
        cmd.Parameters.Add(new SqlParameter("@start", System.Data.SqlDbType.NVarChar, 46) { Value = start is { } s ? s.ToString() : DBNull.Value });
        cmd.Parameters.Add(new SqlParameter("@max", System.Data.SqlDbType.Int) { Value = maxRows });
        SqlDataReader reader;
        try { reader = await cmd.ExecuteReaderAsync(System.Data.CommandBehavior.SequentialAccess, ct); }
        catch (SqlException ex) when (DatabaseIdentity.IsChanged(ex)) { throw new DatabaseChangedException(database, ex); }
        catch (SqlException ex) when (ex.Number == 50002) { throw new LogTruncatedException(start ?? default, Lsn.Parse(ex.Message["log starts at ".Length..].Trim())); }
        await using var r = reader;
        if (!await r.ReadAsync(ct)) throw new InvalidOperationException($"{database}: the log read returned nothing");
        var first = Lsn.Parse(r.GetString(0));
        var zone = TimeZoneInfo.FindSystemTimeZoneById(r.GetString(1));
        await r.NextResultAsync(ct);
        var exact = false;
        while (await r.ReadAsync(ct))
        {
            // the log stores server-local times (sequential access: read in order)
            var lsn = Lsn.Parse(r.GetString(0));
            if (!exact && lsn != first) throw new LogTruncatedException(first, lsn); // truncated between the check and the read
            exact = true;
            yield return new LogRecord
            {
                Lsn = lsn,
                Operation = LogRecord.ParseOperation(r.GetString(1)),
                Context = LogRecord.ParseContext(r.GetString(2)),
                TransactionId = LogRecord.ParseTransactionId(r.GetString(3)),
                Flags = (LogRecordBits)r.GetInt32(4),
                AllocUnitId = r.IsDBNull(5) ? 0 : r.GetInt64(5),
                PartitionId = r.IsDBNull(6) ? 0 : r.GetInt64(6),
                OffsetInRow = r.IsDBNull(7) ? (short)0 : r.GetInt16(7),
                ModifySize = r.IsDBNull(8) ? (short)0 : r.GetInt16(8),
                LogRecordLength = r.IsDBNull(9) ? 0 : r.GetInt16(9),
                RowLog0 = r.IsDBNull(10) ? null : r.GetFieldValue<byte[]>(10),
                RowLog1 = r.IsDBNull(11) ? null : r.GetFieldValue<byte[]>(11),
                RowLog2 = r.IsDBNull(12) ? null : r.GetFieldValue<byte[]>(12),
                TransactionBegin = r.IsDBNull(13) ? default : Lsn.Parse(r.GetString(13)),
                EndTime = r.IsDBNull(14) ? null : LogRecord.ParseLogTime(r.GetString(14)) is { } local ? ToUtc(local, zone) : null,
                UndoneAfter = r.IsDBNull(15) ? default : Lsn.Parse(r.GetString(15)),
                ReplicatedRecords = r.IsDBNull(16) ? -1 : r.GetInt64(16),
            };
        }
        if (!exact) throw new InvalidOperationException($"{database}: record {first} is not in the log");
        // the check behind the rows has no result set of its own: running into the end of the batch raises its error
        try { while (await r.NextResultAsync(ct)) { } }
        catch (SqlException ex) when (DatabaseIdentity.IsChanged(ex)) { throw new DatabaseChangedException(database, ex); }
    }
}
