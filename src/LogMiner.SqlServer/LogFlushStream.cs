using System.Data;
using System.Text.RegularExpressions;
using LogMiner.Log;
using Microsoft.Data.SqlClient;

namespace LogMiner.SqlServer;

/// <summary>
/// Log flushes of every database, pushed by the engine itself: an extended-event session on <c>log_flush_complete</c> whose live stream is
/// read over an ordinary connection. Needs no Windows privilege (unlike ETW), no target on disk, and costs the same for one database or
/// 1,200.
/// <para>
/// SQL Server hands events to a live stream only when a buffer is full or after MAX_DISPATCH_LATENCY (1 s at least). Reading
/// <c>sys.dm_xe_session_targets</c> for the session makes it dispatch at once (measured: experiments/22, 55 µs of server CPU when filtered to
/// this session's address, 28 ms when not), so that is done every <paramref name="dispatchInterval"/>.
/// </para>
/// <para>
/// Side effect: the engine then dispatches the buffers of <i>every</i> event session (experiments/24). In-memory targets do not care; a session
/// with a file target (a trace, SQL Server Audit) writes what it has collected that often instead of when its buffer is full - small writes, a
/// few percent more file space (experiments/24b). <see cref="NeighboursAsync"/> tells whom it concerns.
/// </para>
/// Events are allowed to be lost (ALLOW_SINGLE_EVENT_LOSS): the engine must never wait for the miner.
/// </summary>
internal sealed partial class LogFlushStream(string connectionString, string session, TimeSpan dispatchInterval)
{
    private readonly string _session = SessionName().IsMatch(session) ? session : throw new ArgumentException($"invalid event session name '{session}'", nameof(session));

    [GeneratedRegex("^[A-Za-z][A-Za-z0-9_]{0,99}$")] private static partial Regex SessionName();

    /// <summary>Attaches and reads until cancelled (returns) or until anything goes wrong (throws). The session is dropped at the end.</summary>
    /// <param name="attached">Called once, when the stream is understood.</param>
    /// <param name="flushed">Called on the reading thread for every packet with flushes; the list is reused.</param>
    public async Task RunAsync(Action attached, Action<List<LogFlush>> flushed, CancellationToken ct)
    {
        await using var control = new SqlConnection(connectionString);
        await control.OpenAsync(ct);
        var decoder = await DecoderAsync(control, ct);
        await StartSessionAsync(control, ct);
        try
        {
            using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
            var address = await AddressAsync(control, ct);
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(ct);
            await using var cmd = new SqlCommand("SELECT type, data FROM sys.fn_MSxe_read_event_stream(@source, 0) /*logminer-doorbell-stream*/", conn) { CommandTimeout = 0 };
            cmd.Parameters.Add("@source", SqlDbType.NVarChar, 256).Value = _session;
            var reading = ReadAsync(cmd, decoder, attached, flushed, stop.Token);
            var dispatching = DispatchAsync(control, address, stop.Token);
            var first = await Task.WhenAny(reading, dispatching);
            await stop.CancelAsync();
            // the one that ended first has the reason; what the other one says is only a consequence
            try { await Task.WhenAll(reading, dispatching); }
            catch (Exception) { }
            if (!ct.IsCancellationRequested) await first;
        }
        finally { await DropSessionAsync(); }
    }

    private static async Task ReadAsync(SqlCommand cmd, LogFlushDecoder decoder, Action attached, Action<List<LogFlush>> flushed, CancellationToken ct)
    {
        await using var r = await cmd.ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct);
        var flushes = new List<LogFlush>();
        var ready = false;
        while (await r.ReadAsync(ct))
        {
            flushes.Clear();
            var type = r.GetInt32(0);
            decoder.Decode(type, r.GetFieldValue<byte[]>(1), flushes);
            if (!ready && decoder.Ready)
            {
                ready = true;
                attached();
            }
            if (flushes.Count > 0) flushed(flushes);
        }
        ct.ThrowIfCancellationRequested();
        throw new IOException("the live event stream ended");
    }

    private async Task DispatchAsync(SqlConnection conn, byte[] address, CancellationToken ct)
    {
        await using var cmd = new SqlCommand("SELECT COUNT(*) FROM sys.dm_xe_session_targets WHERE event_session_address = @address /*logminer-doorbell-dispatch*/", conn) { CommandTimeout = 10 };
        cmd.Parameters.Add("@address", SqlDbType.VarBinary, 8).Value = address;
        await Task.Delay(50, ct); // the stream attaches
        while (true)
        {
            // the live stream is a target of the session: none means that it was detached, or that the session is gone
            if ((int)(await cmd.ExecuteScalarAsync(ct))! == 0) throw new IOException($"nothing is attached to event session {_session} any more");
            await Task.Delay(dispatchInterval, ct);
        }
    }

    private static async Task<LogFlushDecoder> DecoderAsync(SqlConnection conn, CancellationToken ct)
    {
        var columns = new List<(string, string)>();
        await using var cmd = new SqlCommand("SELECT name, type_name FROM sys.dm_xe_object_columns WHERE object_name = @event AND column_type = N'data' ORDER BY column_id", conn);
        cmd.Parameters.AddWithValue("@event", LogFlushDecoder.EventName);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct)) columns.Add((r.GetString(0), r.GetString(1)));
        return new LogFlushDecoder(columns);
    }

    private async Task StartSessionAsync(SqlConnection conn, CancellationToken ct)
    {
        await using var cmd = new SqlCommand(StartSql(_session), conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    internal static string StartSql(string session) => $"""
        IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = N'{session}') DROP EVENT SESSION {session} ON SERVER;
        CREATE EVENT SESSION {session} ON SERVER
        ADD EVENT sqlserver.{LogFlushDecoder.EventName} (WHERE database_id > 4)
        WITH (MAX_MEMORY = 2048 KB, EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS, MAX_DISPATCH_LATENCY = 1 SECONDS, MEMORY_PARTITION_MODE = NONE,
              TRACK_CAUSALITY = OFF, STARTUP_STATE = OFF);
        ALTER EVENT SESSION {session} ON SERVER STATE = START;
        """;

    /// <summary>Other running sessions that write files (those SQL Server ships collect next to nothing and are left out).</summary>
    public static async Task<IReadOnlyList<string>> NeighboursAsync(string connectionString, CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("""
            SELECT DISTINCT s.name FROM sys.server_event_sessions s JOIN sys.server_event_session_targets t ON t.event_session_id = s.event_session_id
            JOIN sys.dm_xe_sessions r ON r.name = s.name
            WHERE t.name LIKE N'%file%' AND s.name NOT IN (N'system_health', N'AlwaysOn_health', N'hkenginexesession')
            UNION SELECT a.name FROM sys.server_audits a WHERE a.is_state_enabled = 1
            """, conn);
        var names = new List<string>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct)) names.Add(r.GetString(0));
        return names;
    }

    private async Task<byte[]> AddressAsync(SqlConnection conn, CancellationToken ct)
    {
        await using var cmd = new SqlCommand("SELECT address FROM sys.dm_xe_sessions WHERE name = @session", conn);
        cmd.Parameters.AddWithValue("@session", _session);
        return await cmd.ExecuteScalarAsync(ct) as byte[] ?? throw new IOException($"event session {_session} is not running");
    }

    /// <summary>Best effort, also while shutting down: a session left behind costs next to nothing and is replaced on the next start.</summary>
    private async Task DropSessionAsync()
    {
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(3));
            await using var conn = new SqlConnection(connectionString);
            await conn.OpenAsync(timeout.Token);
            await using var cmd = new SqlCommand($"IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = N'{_session}') DROP EVENT SESSION {_session} ON SERVER;", conn) { CommandTimeout = 3 };
            await cmd.ExecuteNonQueryAsync(timeout.Token);
        }
        catch (Exception ex) when (ex is SqlException or InvalidOperationException or OperationCanceledException) { }
    }
}
