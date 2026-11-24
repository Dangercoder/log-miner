using System.Text.Json;
using LogMiner.Pipeline;
using Microsoft.Data.SqlClient;

namespace LogMiner.SqlServer;

/// <summary>
/// Keeps the miner's position in SQL Server (a small dedicated database, e.g. "logminer", on the same instance), and
/// provides single-writer fencing.
/// <list type="bullet">
/// <item><b>Lease.</b> An exclusive session application lock (<c>sp_getapplock</c>) on a dedicated connection. A second
/// instance (standby on another host) waits; when the active one dies its session ends, the lock is released by SQL Server
/// and the standby takes over from the same checkpoints.</item>
/// <item><b>Fenced checkpoints.</b> Saves run on the lease session and verify inside the batch that the lock is still held,
/// so an instance that lost its lease can never overwrite a newer position.</item>
/// <item><b>Durable before release.</b> Saves commit with delayed durability explicitly off; sp_repldone only follows them.</item>
/// </list>
/// The service is stateless: reinstall or move it and it resumes from here. Even if this table were lost, nothing is lost:
/// each tenant database itself still holds the sp_repldone position, and everything after it is still in its log.
/// </summary>
public sealed class SqlStateStore(string connectionString, string serverName) : ICheckpointStore, IAsyncDisposable
{
    // The lease lives in a session: a pooled connection would keep the session (and the lock) alive after Dispose.
    private readonly string _leaseConnectionString = new SqlConnectionStringBuilder(connectionString) { Pooling = false }.ConnectionString;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private SqlConnection? _conn;
    private Dictionary<string, Checkpoint>? _cache;

    public string Resource { get; } = $"logminer:{serverName}";
    public bool HoldsLease { get; private set; }

    /// <summary>Creates the table if needed and verifies delayed durability is not forced on the state database.</summary>
    public async Task EnsureSchemaAsync(CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("""
            IF (SELECT delayed_durability FROM sys.databases WHERE database_id = DB_ID()) = 2
                THROW 50010, 'delayed durability is FORCED on the logminer state database; checkpoints must be fully durable', 1;
            IF OBJECT_ID(N'dbo.checkpoints', N'U') IS NULL
                CREATE TABLE dbo.checkpoints (
                    server_name   sysname      NOT NULL,
                    database_name sysname      NOT NULL,
                    incarnation   varchar(80)  NOT NULL,
                    restart_lsn   binary(10)   NOT NULL,
                    commit_lsn    binary(10)   NOT NULL,
                    begin_lsn     binary(10)   NOT NULL,
                    updated_at    datetime2(3) NOT NULL,
                    held          bit          NOT NULL CONSTRAINT df_checkpoints_held DEFAULT 0,
                    resumed_at    binary(10)   NULL,
                    resumed_in_gap bit         NOT NULL CONSTRAINT df_checkpoints_in_gap DEFAULT 0,
                    CONSTRAINT pk_checkpoints PRIMARY KEY (server_name, database_name));
            ELSE IF COL_LENGTH(N'dbo.checkpoints', N'held') IS NULL
                ALTER TABLE dbo.checkpoints ADD held bit NOT NULL CONSTRAINT df_checkpoints_held DEFAULT 0;
            IF COL_LENGTH(N'dbo.checkpoints', N'resumed_at') IS NULL
                ALTER TABLE dbo.checkpoints ADD resumed_at binary(10) NULL, resumed_in_gap bit NOT NULL CONSTRAINT df_checkpoints_in_gap DEFAULT 0;
            """, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Waits until this instance holds the lease (standby instances wait here). Returns when acquired.</summary>
    public async Task AcquireLeaseAsync(Action? onWaiting, CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            while (true)
            {
                if (_conn is null || _conn.State != System.Data.ConnectionState.Open)
                {
                    if (_conn is not null) await _conn.DisposeAsync();
                    _conn = new SqlConnection(_leaseConnectionString);
                    await _conn.OpenAsync(ct);
                }
                await using var cmd = new SqlCommand("DECLARE @r int; EXEC @r = sp_getapplock @Resource = @res, @LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0; SELECT @r;", _conn);
                cmd.Parameters.AddWithValue("@res", Resource);
                var result = Convert.ToInt32(await cmd.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
                if (result >= 0) { HoldsLease = true; _cache = null; return; }
                onWaiting?.Invoke();
                _gate.Release();
                try { await Task.Delay(TimeSpan.FromSeconds(2), ct); }
                finally { await _gate.WaitAsync(CancellationToken.None); } // re-enter before the outer finally releases
            }
        }
        finally { _gate.Release(); }
    }

    /// <summary>True while the lease session is alive and still owns the lock.</summary>
    public async Task<bool> CheckLeaseAsync(CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            if (_conn is null || _conn.State != System.Data.ConnectionState.Open) return HoldsLease = false;
            await using var cmd = new SqlCommand("SELECT APPLOCK_MODE('public', @res, 'Session');", _conn);
            cmd.Parameters.AddWithValue("@res", Resource);
            return HoldsLease = (await cmd.ExecuteScalarAsync(ct) as string) == "Exclusive";
        }
        catch (SqlException) { return HoldsLease = false; }
        finally { _gate.Release(); }
    }

    public async Task<Checkpoint?> LoadAsync(string database, CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            if (_cache is null)
            {
                var map = new Dictionary<string, Checkpoint>(StringComparer.OrdinalIgnoreCase);
                await using var conn = new SqlConnection(connectionString);
                await conn.OpenAsync(ct);
                await using var cmd = new SqlCommand("SELECT database_name, incarnation, restart_lsn, commit_lsn, begin_lsn, updated_at, held, resumed_at, resumed_in_gap FROM dbo.checkpoints WHERE server_name = @s", conn);
                cmd.Parameters.AddWithValue("@s", serverName);
                await using var r = await cmd.ExecuteReaderAsync(ct);
                while (await r.ReadAsync(ct))
                {
                    var db = r.GetString(0);
                    map[db] = new Checkpoint(db, r.GetString(1), Lsn.FromBinary(r.GetFieldValue<byte[]>(2)), Lsn.FromBinary(r.GetFieldValue<byte[]>(3)),
                        Lsn.FromBinary(r.GetFieldValue<byte[]>(4)), new DateTimeOffset(DateTime.SpecifyKind(r.GetDateTime(5), DateTimeKind.Utc)), r.GetBoolean(6),
                        r.IsDBNull(7) ? default : Lsn.FromBinary(r.GetFieldValue<byte[]>(7)), r.GetBoolean(8));
                }
                _cache = map;
            }
            return _cache.GetValueOrDefault(database);
        }
        finally { _gate.Release(); }
    }

    /// <summary>Upserts the batch atomically on the lease session; fails (and persists nothing) if the lease is no longer held.</summary>
    public async Task SaveAsync(IReadOnlyCollection<Checkpoint> checkpoints, CancellationToken ct)
    {
        if (checkpoints.Count == 0) return;
        var json = JsonSerializer.Serialize(checkpoints.Select(c => new
        {
            d = c.Database, i = c.Incarnation,
            r = Convert.ToHexString(c.RestartLsn.ToBinary()), c = Convert.ToHexString(c.LastCommitLsn.ToBinary()), b = Convert.ToHexString(c.LastBeginLsn.ToBinary()),
            h = c.Held, a = c.ResumedAt.IsZero ? null : Convert.ToHexString(c.ResumedAt.ToBinary()), g = c.ResumedInGap,
        }));
        await _gate.WaitAsync(ct);
        try
        {
            if (_conn is null || _conn.State != System.Data.ConnectionState.Open || !HoldsLease)
                throw new InvalidOperationException("the logminer lease is not held; refusing to write checkpoints");
            await using var cmd = new SqlCommand("""
                SET XACT_ABORT ON;
                IF APPLOCK_MODE('public', @res, 'Session') <> 'Exclusive' THROW 50011, 'logminer lease lost: another instance may be active', 1;
                BEGIN TRANSACTION;
                MERGE dbo.checkpoints WITH (HOLDLOCK) AS t
                USING (SELECT d, i, CONVERT(binary(10), r, 2) AS r, CONVERT(binary(10), c, 2) AS c, CONVERT(binary(10), b, 2) AS b, h, CONVERT(binary(10), a, 2) AS a, g
                       FROM OPENJSON(@json) WITH (d sysname '$.d', i varchar(80) '$.i', r char(20) '$.r', c char(20) '$.c', b char(20) '$.b', h bit '$.h', a char(20) '$.a', g bit '$.g')) AS s
                ON t.server_name = @server AND t.database_name = s.d
                WHEN MATCHED THEN UPDATE SET incarnation = s.i, restart_lsn = s.r, commit_lsn = s.c, begin_lsn = s.b, updated_at = SYSUTCDATETIME(), held = s.h, resumed_at = s.a, resumed_in_gap = s.g
                WHEN NOT MATCHED THEN INSERT (server_name, database_name, incarnation, restart_lsn, commit_lsn, begin_lsn, updated_at, held, resumed_at, resumed_in_gap)
                                      VALUES (@server, s.d, s.i, s.r, s.c, s.b, SYSUTCDATETIME(), s.h, s.a, s.g);
                COMMIT TRANSACTION WITH (DELAYED_DURABILITY = OFF);
                """, _conn) { CommandTimeout = 30 };
            cmd.Parameters.AddWithValue("@res", Resource);
            cmd.Parameters.AddWithValue("@server", serverName);
            cmd.Parameters.Add(new SqlParameter("@json", System.Data.SqlDbType.NVarChar, -1) { Value = json });
            try { await cmd.ExecuteNonQueryAsync(ct); }
            catch (SqlException ex) when (ex.Number == 50011) { HoldsLease = false; throw new InvalidOperationException(ex.Message, ex); }
            if (_cache is not null) foreach (var c in checkpoints) _cache[c.Database] = c;
        }
        finally { _gate.Release(); }
    }

    public async ValueTask DisposeAsync()
    {
        if (_conn is { State: System.Data.ConnectionState.Open } && HoldsLease)
        {
            try
            {
                await using var cmd = new SqlCommand("EXEC sp_releaseapplock @Resource = @res, @LockOwner = 'Session';", _conn);
                cmd.Parameters.AddWithValue("@res", Resource);
                await cmd.ExecuteNonQueryAsync();
            }
            catch (SqlException) { } // the session is going away anyway
        }
        HoldsLease = false;
        if (_conn is not null) await _conn.DisposeAsync(); // unpooled: closing ends the session
        _gate.Dispose();
    }
}
