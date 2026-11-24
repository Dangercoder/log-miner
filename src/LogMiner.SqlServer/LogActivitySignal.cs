using System.Runtime.CompilerServices;
using Microsoft.Data.SqlClient;

namespace LogMiner.SqlServer;

/// <summary>A database wrote to its log.</summary>
/// <param name="Block">The newest log block known to have been written (zero: unknown).</param>
public readonly record struct LogActivity(int DatabaseId, Lsn Block);

/// <summary>Tells the capture engine which databases wrote to their transaction log ("doorbell").</summary>
public interface ILogActivitySignal
{
    /// <summary>Yields sets of databases whose log advanced since the previous set. An empty set means every database.</summary>
    IAsyncEnumerable<IReadOnlyCollection<LogActivity>> WatchAsync(CancellationToken ct);
}

/// <summary>
/// Polls sys.dm_io_virtual_file_stats for log files. Works everywhere with VIEW SERVER STATE, but costs
/// ~2 µs of SQL Server CPU per log file per poll (measured: 4.5 ms per poll for 1,209 databases), so the
/// interval is a CPU/latency trade-off. The simplest possible signal, for tests; the service uses <see cref="DoorbellSignal"/>.
/// </summary>
public sealed class DmvPollSignal(string connectionString, TimeSpan interval) : ILogActivitySignal
{
    private const string Sql = "SELECT database_id, num_of_bytes_written FROM sys.dm_io_virtual_file_stats(NULL, 2) WHERE database_id > 4 /*logminer-doorbell*/";

    public TimeSpan Interval { get; } = interval;
    public long Polls { get; private set; }
    public DateTimeOffset LastPoll { get; private set; }

    public async IAsyncEnumerable<IReadOnlyCollection<LogActivity>> WatchAsync([EnumeratorCancellation] CancellationToken ct)
    {
        var last = new Dictionary<int, long>();
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand(Sql, conn) { CommandTimeout = 30 };
        using var timer = new PeriodicTimer(Interval);
        var first = true;
        while (!ct.IsCancellationRequested)
        {
            var changed = new List<LogActivity>();
            await using (var r = await cmd.ExecuteReaderAsync(ct))
            {
                while (await r.ReadAsync(ct))
                {
                    var db = (int)r.GetInt16(0); // database_id is smallint in this DMV
                    var bytes = r.GetInt64(1);
                    if (!last.TryGetValue(db, out var prev) || prev != bytes) changed.Add(new LogActivity(db, default));
                    last[db] = bytes;
                }
            }
            Polls++;
            LastPoll = DateTimeOffset.UtcNow;
            if (changed.Count > 0 || first) yield return changed;
            first = false;
            if (!await timer.WaitForNextTickAsync(ct)) yield break;
        }
    }
}
