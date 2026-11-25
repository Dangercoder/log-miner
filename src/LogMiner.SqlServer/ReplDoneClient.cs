using System.Globalization;
using LogMiner.Capture;
using LogMiner.Pipeline;
using Microsoft.Data.SqlClient;

namespace LogMiner.SqlServer;

/// <summary>
/// Marks transactions as "distributed" with sp_repldone so SQL Server may truncate the log behind them.
/// CDC-enabled tables mark their records as replicated, which holds log truncation (log_reuse_wait = REPLICATION)
/// until someone - normally the CDC capture job, here the miner - calls sp_repldone. The miner only calls it with a
/// position whose events are acknowledged by the sink and whose checkpoint is durable, so an outage of the miner
/// retains (never loses) log. sp_replflush releases the per-database log reader role taken by the call.
/// </summary>
public static class ReplDoneClient
{
    /// <summary>Begin LSN of the oldest replicated transaction that has not been released (null: none), or a little before: the engine reports a log
    /// block, sometimes that of a checkpoint which ran just before the transaction began (measured). Never later. Reads persistent state; does not
    /// take the log reader role.</summary>
    public static async Task<Lsn?> OldestUnreleasedAsync(SqlConnection conn, DatabaseRef database, CancellationToken ct)
    {
        await using var cmd = Inside(conn, database, "DBCC OPENTRAN WITH TABLERESULTS, NO_INFOMSGS;");
        Lsn? oldest = null;
        try
        {
            await using var r = await cmd.ExecuteReaderAsync(ct);
            while (await r.ReadAsync(ct))
            {
                if (r.GetString(0) != "REPL_NONDIST_OLD_LSN") continue;
                var p = r.GetString(1).Trim('(', ')').Split(':'); // decimal "(vlf:block:slot)"
                var lsn = new Lsn(uint.Parse(p[0], CultureInfo.InvariantCulture), uint.Parse(p[1], CultureInfo.InvariantCulture), ushort.Parse(p[2], CultureInfo.InvariantCulture));
                oldest = lsn.IsZero ? null : lsn;
            }
            while (await r.NextResultAsync(ct)) { }
        }
        catch (SqlException ex) when (DatabaseIdentity.IsChanged(ex)) { throw new DatabaseChangedException(database, ex); }
        return oldest;
    }

    private static SqlCommand Inside(SqlConnection conn, DatabaseRef database, string statement)
    {
        var cmd = DatabaseIdentity.Inside(conn, database, statement, ", @x binary(10), @s binary(10)", ", @x, @s");
        cmd.CommandTimeout = 30;
        cmd.Parameters.Add(new SqlParameter("@x", System.Data.SqlDbType.Binary, 10) { Value = DBNull.Value });
        cmd.Parameters.Add(new SqlParameter("@s", System.Data.SqlDbType.Binary, 10) { Value = DBNull.Value });
        return cmd;
    }

    /// <summary>Why the held-back transaction of <paramref name="position"/> is no longer unreleased, or null when it still is.</summary>
    public static string? Explain(Lsn? oldestUnreleased, Checkpoint position) =>
        oldestUnreleased is null ? $"SQL Server has no unreleased transaction although {position.LastCommitLsn} was held back: someone else released the log (sp_repldone, a CDC capture job, a Log Reader Agent) or CDC was disabled"
        : oldestUnreleased.Value > position.LastBeginLsn ? $"SQL Server's oldest unreleased transaction began at {oldestUnreleased} although the one at {position.LastBeginLsn} was held back: someone else released the log (sp_repldone, a CDC capture job, a Log Reader Agent) or CDC was disabled and enabled again"
        : null;

    public static async Task MarkDistributedAsync(SqlConnection conn, DatabaseRef database, Lsn beginLsn, Lsn commitLsn, CancellationToken ct)
    {
        await using var cmd = Inside(conn, database, "EXEC sys.sp_repldone @xactid = @x, @xact_seqno = @s, @numtrans = 0, @time = 0, @reset = 0; EXEC sys.sp_replflush;");
        cmd.Parameters["@x"].Value = beginLsn.ToBinary();
        cmd.Parameters["@s"].Value = commitLsn.ToBinary();
        try { await cmd.ExecuteNonQueryAsync(ct); }
        catch (SqlException ex) when (DatabaseIdentity.IsChanged(ex)) { throw new DatabaseChangedException(database, ex); }
    }
}
