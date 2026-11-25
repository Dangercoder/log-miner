using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using LogMiner.Capture;
using LogMiner.Events;
using LogMiner.Pipeline;
using LogMiner.Sinks;
using Microsoft.Data.SqlClient;

namespace LogMiner.SqlServer;

/// <param name="DoorbellProblem">Why log flushes are not pushed on this build. Not a reason to fail: the service polls instead.</param>
public sealed record SelfTestResult(bool Passed, string ProductVersion, IReadOnlyList<string> Problems, TimeSpan Elapsed, string? DoorbellProblem = null);

/// <summary>
/// Proves on this exact SQL Server build that everything the miner relies on still holds: reading the log, decoding every type, undo
/// records, layouts as of an LSN across DDL, historical loads, and the release bookmark. The log's format is not a documented interface and
/// a cumulative update may change it; the service runs this once per server version and miner build and refuses to capture when it fails.
/// <para>Works in its own small scratch database, which is dropped afterwards.</para>
/// </summary>
public static class SelfTest
{
    public const string DatabaseName = "logminer_selftest";
    private const string Marker = "logminer_selftest_scratch";

    private const string Tables = """
        CREATE TABLE dbo.typed (id int NOT NULL CONSTRAINT pk_typed PRIMARY KEY, c_tinyint tinyint NULL, c_smallint smallint NULL, c_int int NULL, c_bigint bigint NULL, c_bit bit NULL, c_bit2 bit NULL,
          c_dec decimal(18,4) NULL, c_dec38 decimal(38,10) NULL, c_num numeric(5,2) NULL, c_money money NULL, c_smallmoney smallmoney NULL, c_float float NULL, c_real real NULL, c_date date NULL, c_time time(7) NULL,
          c_time0 time(0) NULL, c_dt2 datetime2(7) NULL, c_dt2_3 datetime2(3) NULL, c_dto datetimeoffset(7) NULL, c_dt datetime NULL, c_sdt smalldatetime NULL, c_char char(10) NULL, c_varchar varchar(100) NULL,
          c_cyr varchar(50) COLLATE Cyrillic_General_CI_AS NULL, c_nchar nchar(10) NULL, c_nvarchar nvarchar(100) NULL, c_binary binary(8) NULL, c_varbinary varbinary(100) NULL, c_guid uniqueidentifier NULL,
          c_vmax nvarchar(max) NULL, c_rv rowversion NOT NULL);
        CREATE TABLE dbo.heap (v int NULL, id int NOT NULL CONSTRAINT pk_heap PRIMARY KEY NONCLUSTERED, s varchar(2000) NULL);
        CREATE TABLE dbo.ddl (v int NULL, s varchar(40) NULL, id int NOT NULL CONSTRAINT pk_ddl PRIMARY KEY);
        """;

    private const string TypedRows = """
        INSERT dbo.typed (id, c_tinyint, c_smallint, c_int, c_bigint, c_bit, c_bit2, c_dec, c_dec38, c_num, c_money, c_smallmoney, c_float, c_real, c_date, c_time, c_time0, c_dt2, c_dt2_3, c_dto, c_dt, c_sdt,
                          c_char, c_varchar, c_cyr, c_nchar, c_nvarchar, c_binary, c_varbinary, c_guid, c_vmax)
        VALUES (1, 7, -5, 123456, -9876543210, 1, 0, 12345.6789, -1234567890123456789012345678.0123456789, 999.99, 922337203685477.5807, -214748.3648, 3.141592653589793, -2.5, '2026-11-24', '23:59:59.9999999', '12:34:56',
                '9999-12-31T23:59:59.9999999', '2026-11-24T01:02:03.456', '2026-11-24T01:02:03.4567891+05:30', '2026-11-24T01:02:03.457', '2026-11-24T01:02:00', 'abc', 'plain', N'Привет', N'åäö', N'漢字 – ☃', 0x0102030405060708,
                0xFF00FF, '00112233-4455-6677-8899-AABBCCDDEEFF', REPLICATE(CONVERT(nvarchar(max), N'x'), 1000)),
               (3, 0, -32768, -2147483648, -9223372036854775808, 0, 1, -99999999999999.9999, 0, -999.99, -922337203685477.5808, 214748.3647, -1.7E308, 3.0E38, '0001-01-01', '00:00:00', '00:00:00', '0001-01-01', '0001-01-01',
                '0001-01-01T00:00:00+00:00', '1753-01-01', '1900-01-01', '', '', N'', N'', N'', 0x00, 0x, '00000000-0000-0000-0000-000000000000', N'');
        INSERT dbo.typed (id) VALUES (2);
        INSERT dbo.typed (id, c_int, c_varchar) SELECT value, value, REPLICATE('v', value) FROM (VALUES (10), (11), (12), (13), (14)) x(value);
        UPDATE dbo.typed SET c_int = c_int + 1, c_varchar = 'changed', c_nvarchar = NULL WHERE id IN (1, 11);
        UPDATE dbo.typed SET id = 112 WHERE id = 12;
        DELETE dbo.typed WHERE id = 13;
        BEGIN TRAN; INSERT dbo.typed (id, c_int) VALUES (900, 1); UPDATE dbo.typed SET c_int = -1; ROLLBACK;
        BEGIN TRAN; INSERT dbo.typed (id, c_int) VALUES (20, 1); SAVE TRAN s; INSERT dbo.typed (id, c_int) VALUES (901, 1); DELETE dbo.typed WHERE id = 1; UPDATE dbo.typed SET c_vmax = REPLICATE(CONVERT(nvarchar(max), N'y'), 9000) WHERE id = 20; ROLLBACK TRAN s; UPDATE dbo.typed SET c_int = 2 WHERE id = 20; COMMIT;
        BEGIN TRY BEGIN TRAN; INSERT dbo.typed (id, c_int) VALUES (21, 1); BEGIN TRY INSERT dbo.typed (id) SELECT value FROM (VALUES (22), (1)) x(value); END TRY BEGIN CATCH END CATCH; IF XACT_STATE() = 1 COMMIT; ELSE ROLLBACK; END TRY BEGIN CATCH IF @@TRANCOUNT > 0 ROLLBACK; END CATCH;
        INSERT dbo.heap (id, v, s) SELECT value, value, REPLICATE('h', 100) FROM (VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16), (17), (18), (19), (20), (21), (22), (23), (24), (25), (26), (27), (28), (29), (30), (31), (32), (33), (34), (35), (36), (37), (38), (39), (40), (41), (42), (43), (44), (45), (46), (47), (48), (49), (50), (51), (52), (53), (54), (55), (56), (57), (58), (59), (60), (61), (62), (63), (64)) x(value);
        UPDATE dbo.heap SET s = REPLICATE('f', 1900) WHERE id % 9 = 0;
        UPDATE dbo.heap SET v = v + 1 WHERE id % 9 = 0;
        DELETE dbo.heap WHERE id IN (1, 18);
        """;

    /// <summary>Changes made while the capture is "down", with DDL in between: every record must be decoded with the layout of its moment.</summary>
    private const string DdlScript = """
        INSERT dbo.ddl (id, v, s) VALUES (1, 10, 'one'), (2, 20, 'two');
        EXEC('ALTER TABLE dbo.ddl ALTER COLUMN v bigint NULL;');
        EXEC('UPDATE dbo.ddl SET v = 5000000000 WHERE id = 1;');
        EXEC('ALTER INDEX ALL ON dbo.ddl REBUILD;');
        EXEC('INSERT dbo.ddl (id, v, s) VALUES (3, 30, ''three'');');
        EXEC('ALTER TABLE dbo.ddl ADD extra int NOT NULL CONSTRAINT df_extra DEFAULT 5 WITH VALUES;');
        EXEC('UPDATE dbo.ddl SET s = ''deux'' WHERE id = 2;');
        EXEC('ALTER TABLE dbo.ddl ALTER COLUMN s varchar(40) COLLATE Cyrillic_General_CI_AS NULL;');
        EXEC('UPDATE dbo.ddl SET s = N''три'' WHERE id = 3;');
        EXEC('BEGIN TRAN; INSERT dbo.ddl (id, v, s) VALUES (4, 40, ''four''); SAVE TRAN sp; ALTER TABLE dbo.ddl ADD ghost int NULL; UPDATE dbo.ddl SET v = -1; ROLLBACK TRAN sp; ALTER TABLE dbo.ddl DROP CONSTRAINT df_extra, COLUMN extra; INSERT dbo.ddl (id, v, s) VALUES (5, 50, ''five''); COMMIT;');
        EXEC('ALTER TABLE dbo.ddl REBUILD;');
        EXEC('DELETE dbo.ddl WHERE id = 2;');
        """;

    private static readonly string[] DdlExpected =
    [
        """c|{"v":10,"s":"one","id":1}""", """c|{"v":20,"s":"two","id":2}""",
        """u|{"v":5000000000,"s":"one","id":1}""",
        """c|{"v":30,"s":"three","id":3}""",
        """u|{"v":20,"s":"deux","id":2,"extra":5}""",
        """u|{"v":30,"s":"три","id":3,"extra":5}""",
        """c|{"v":40,"s":"four","id":4,"extra":5}""", """c|{"v":50,"s":"five","id":5}""",
        """d|{"v":20,"s":"deux","id":2}""",
    ];

    public static async Task<SelfTestResult> RunAsync(string connectionString, CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        var problems = new List<string>();
        string? doorbell = null;
        var version = await VersionAsync(connectionString, ct);
        var master = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = "master" }.ConnectionString;
        var scratch = new SqlConnectionStringBuilder(connectionString) { InitialCatalog = DatabaseName, Pooling = false }.ConnectionString;
        try
        {
            await DropAsync(master, ct);
            await CreateAsync(master, scratch, ct);
            await CheckAsync(master, scratch, problems, ct);
            doorbell = await CheckDoorbellAsync(master, scratch, ct);
        }
        catch (Exception ex) when (!ct.IsCancellationRequested) { problems.Add($"{ex.GetType().Name}: {ex.Message}"); }
        finally
        {
            try { await DropAsync(master, CancellationToken.None); }
            catch (Exception ex) when (ex is SqlException or InvalidOperationException) { problems.Add($"the scratch database could not be dropped: {ex.Message}"); }
        }
        return new SelfTestResult(problems.Count == 0, version, problems, sw.Elapsed, doorbell);
    }

    public static async Task<string> VersionAsync(string connectionString, CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("SELECT CONCAT(CONVERT(varchar(30), SERVERPROPERTY('ProductVersion')), ' ', CONVERT(varchar(30), SERVERPROPERTY('ProductUpdateLevel')))", conn);
        return ((string)(await cmd.ExecuteScalarAsync(ct))!).Trim();
    }

    private static async Task CreateAsync(string master, string scratch, CancellationToken ct)
    {
        await ExecAsync(master, $"CREATE DATABASE {DatabaseName}; ALTER DATABASE {DatabaseName} SET RECOVERY FULL;", ct);
        await ExecAsync(scratch, $"EXEC sys.sp_addextendedproperty @name = N'{Marker}', @value = 1;", ct);
        await ExecAsync(scratch, Tables, ct);
        await ExecAsync(scratch, """
            EXEC sys.sp_cdc_enable_db;
            EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'typed', @role_name = NULL;
            EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'heap', @role_name = NULL;
            EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'ddl', @role_name = NULL;
            """, ct);
        foreach (var job in (string[])["capture", "cleanup"])
            await ExecAsync(scratch, $"IF EXISTS (SELECT 1 FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N'{job}') EXEC sys.sp_cdc_drop_job @job_type = N'{job}';", ct);
        await ExecAsync(master, $"BACKUP DATABASE {DatabaseName} TO DISK = 'NUL';", ct); // the log is kept from here on, as in production
    }

    /// <summary>Drops the scratch database, and only a database that carries our marker.</summary>
    private static async Task DropAsync(string master, CancellationToken ct) => await ExecAsync(master, $"""
        IF DB_ID(N'{DatabaseName}') IS NOT NULL
        BEGIN
            DECLARE @mine int;
            EXEC {DatabaseName}.sys.sp_executesql N'SELECT @mine = COUNT(*) FROM sys.extended_properties WHERE class = 0 AND name = N''{Marker}''', N'@mine int OUTPUT', @mine OUTPUT;
            IF @mine = 0 THROW 50020, 'a database named {DatabaseName} exists but was not created by the logminer self-test; not touching it', 1;
            ALTER DATABASE {DatabaseName} SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
            DROP DATABASE {DatabaseName};
        END
        """, ct);

    private static async Task CheckAsync(string master, string scratch, List<string> problems, CancellationToken ct)
    {
        var (dbId, incarnation) = await IdentityAsync(master, ct);
        await using var reader = new SqlConnection(master);
        await reader.OpenAsync(ct);
        var source = new EngineLogSource(reader, master, CaptureScope.CdcTrackedTables);
        var snapshots = new SqlSnapshotSource(master);
        var encoder = new EventEncoder(new EncoderOptions { ServerName = "selftest" });
        var settings = new CaptureSettings { StartPosition = StartPosition.Earliest, MaxRowsPerRead = 64, OnGap = GapPolicy.Continue };
        var sink = new MemorySink();
        var store = new MemoryCheckpointStore();
        await using var releaser = new SqlLogReleaser(master);
        var capture = new DatabaseCapture(dbId, DatabaseName, incarnation, null, StartPosition.Earliest);

        async Task PumpAsync(ISnapshotSource? s = null) { while (await capture.PumpAsync(source, encoder, sink, settings, ct, s)) { } }
        async Task CheckpointAsync() => await CheckpointCoordinator.FlushAsync([capture], store, releaser, TimeSpan.Zero, (_, ex) => problems.Add($"sp_repldone: {ex.Message}"), ct);

        // 1. reading and decoding: every type, rollbacks, key changes, a heap with forwarded rows
        await ExecAsync(scratch, TypedRows, ct);
        await PumpAsync();
        var replica = Replica(sink.Snapshot());
        foreach (var table in (string[])["typed", "heap"])
            problems.AddRange(Differences(table, await RowsAsync(capture, snapshots, table, ct), replica.GetValueOrDefault(table) ?? []).Take(5));
        await CheckpointAsync();

        // 2. the release bookmark
        await ExecAsync(scratch, "INSERT dbo.typed (id, c_int) VALUES (30, 1);", ct);
        await PumpAsync();
        await CheckpointAsync();
        await ExecAsync(scratch, "UPDATE dbo.typed SET c_int = 2 WHERE id = 30;", ct);
        await PumpAsync();
        await CheckpointAsync();
        var position = await store.LoadAsync(DatabaseName, ct);
        if (position is not { Held: true }) problems.Add("the newest transaction is not held back");
        else if (ReplDoneClient.Explain(await ReplDoneClient.OldestUnreleasedAsync(reader, capture.Database, ct), position) is { } why) problems.Add($"release bookmark: {why}");
        await ExecAsync(master, $"BACKUP LOG {DatabaseName} TO DISK = 'NUL';", ct);

        // 3. layouts as of an LSN: DDL while the capture is down, then a restart
        var before = sink.Snapshot().Count;
        await ExecAsync(scratch, DdlScript, ct);
        capture = new DatabaseCapture(dbId, DatabaseName, incarnation, position, StartPosition.Earliest);
        await PumpAsync();
        var got = sink.Snapshot().Skip(before).Select(m =>
        {
            using var doc = JsonDocument.Parse(m.Payload);
            var op = doc.RootElement.GetProperty("op").GetString();
            return $"{op}|{Unescaped(doc.RootElement.GetProperty(op == "d" ? "before" : "after"))}";
        }).ToArray();
        for (var i = 0; i < Math.Max(got.Length, DdlExpected.Length); i++)
        {
            var (want, have) = (i < DdlExpected.Length ? DdlExpected[i] : "<nothing>", i < got.Length ? got[i] : "<nothing>");
            if (want != have) { problems.Add($"DDL history, change {i + 1}: expected {want}, got {have}"); break; }
        }

        if (capture.Gaps > 0) problems.Add("a gap was reported although nothing was lost");

        // 4. historical loads encode rows exactly like the stream
        replica = Replica(sink.Snapshot());
        var load = new TableSnapshot(capture.Schema!.Tables.First(t => t.Table == "typed"), "selftest", 3);
        capture.RequestSnapshot(load);
        before = sink.Snapshot().Count;
        for (var i = 0; i < 100 && !load.Done; i++) await PumpAsync(snapshots);
        if (!load.Done || load.Failed) problems.Add($"historical load did not finish: {load.Error}");
        problems.AddRange(MarkerProblems(sink.Snapshot().Skip(before)));
        problems.AddRange(Differences("typed (historical load)", Replica(sink.Snapshot().Skip(before)).GetValueOrDefault("typed") ?? [], replica.GetValueOrDefault("typed") ?? []).Take(5));
    }

    /// <summary>Null when commits are pushed at once, otherwise what is wrong. The stream's format and that its dispatch can be forced are
    /// not documented either, but only latency depends on them.</summary>
    private static async Task<string?> CheckDoorbellAsync(string master, string scratch, CancellationToken ct)
    {
        var (dbId, _) = await IdentityAsync(master, ct);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var attached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        TaskCompletionSource? bell = null;
        var stream = Task.CompletedTask;
        try
        {
            stream = new LogFlushStream(master, DatabaseName, TimeSpan.FromMilliseconds(4)).RunAsync(() => attached.TrySetResult(), flushes =>
            {
                if (flushes.Exists(f => f.DatabaseId == dbId)) Volatile.Read(ref bell)?.TrySetResult();
            }, stop.Token);
            if (await Task.WhenAny(attached.Task, stream, Task.Delay(5000, ct)) != attached.Task)
            {
                if (stream.IsCompleted) await stream;
                return "the live event stream did not start within 5 s";
            }
            await Task.Delay(100, ct);
            for (var i = 0; i < 5; i++)
            {
                var ring = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                Volatile.Write(ref bell, ring);
                await ExecAsync(scratch, $"UPDATE dbo.ddl SET v = {100 + i} WHERE id = 1;", ct);
                if (await Task.WhenAny(ring.Task, stream, Task.Delay(500, ct)) != ring.Task)
                {
                    if (stream.IsCompleted) await stream;
                    return "a commit was not pushed within 500 ms";
                }
            }
            return null;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested) { return $"{ex.GetType().Name}: {ex.Message}"; }
        finally
        {
            await stop.CancelAsync();
            try { await stream; }
            catch (Exception) { } // reported above
        }
    }

    /// <summary>A load is announced before its first row and concluded behind its last, with their number.</summary>
    private static IEnumerable<string> MarkerProblems(IEnumerable<SinkMessage> messages)
    {
        var what = messages.Select(m =>
        {
            using var doc = JsonDocument.Parse(m.Payload);
            var op = doc.RootElement.GetProperty("op").GetString()!;
            return op != "l" ? op : doc.RootElement.GetProperty("load").GetProperty("phase").GetString() + (doc.RootElement.GetProperty("load").TryGetProperty("rows", out var n) ? $" {n.GetInt64()}" : "");
        }).Where(x => x is not ("c" or "u" or "d")).ToList();
        var rows = what.Count(x => x == "r");
        if (what.Count != rows + 2 || what[0] != "begin" || what[^1] != $"end {rows}") yield return $"the markers of a historical load are wrong: {string.Join(" ", what.Where(x => x != "r"))}, {rows} rows";
    }

    /// <summary>table → id → column → JSON, after applying the events in order.</summary>
    private static Dictionary<string, Dictionary<string, Dictionary<string, string>>> Replica(IEnumerable<SinkMessage> messages)
    {
        var tables = new Dictionary<string, Dictionary<string, Dictionary<string, string>>>();
        foreach (var m in messages)
        {
            using var doc = JsonDocument.Parse(m.Payload);
            var root = doc.RootElement;
            var op = root.GetProperty("op").GetString();
            if (op == "l") continue;
            var image = root.GetProperty(op == "d" ? "before" : "after");
            var table = root.GetProperty("source").GetProperty("table").GetString()!;
            if (!tables.TryGetValue(table, out var rows)) tables[table] = rows = [];
            var id = image.GetProperty("id").GetRawText();
            if (op == "d") rows.Remove(id); else rows[id] = image.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetRawText());
        }
        return tables;
    }

    private static async Task<Dictionary<string, Dictionary<string, string>>> RowsAsync(DatabaseCapture capture, SqlSnapshotSource snapshots, string table, CancellationToken ct)
    {
        var layout = capture.Schema!.Tables.First(t => t.Table == table);
        var chunk = await snapshots.ReadChunkAsync(layout, null, 10_000, ct);
        var rows = new Dictionary<string, Dictionary<string, string>>();
        foreach (var r in chunk.Rows)
        {
            var row = new Dictionary<string, string>();
            for (var i = 0; i < layout.Visible.Count; i++) row[layout.Visible[i].Name] = Json(r.Values[i]);
            rows[row["id"]] = row;
        }
        return rows;
    }

    private static IEnumerable<string> Differences(string table, Dictionary<string, Dictionary<string, string>> expected, Dictionary<string, Dictionary<string, string>> actual)
    {
        if (expected.Count == 0) yield return $"{table}: no rows to compare";
        foreach (var id in expected.Keys.Union(actual.Keys))
        {
            if (!actual.TryGetValue(id, out var have)) { yield return $"{table} row {id} is missing from the events"; continue; }
            if (!expected.TryGetValue(id, out var want)) { yield return $"{table} row {id} is in the events but not in the table"; continue; }
            foreach (var column in want.Keys.Union(have.Keys))
            {
                var (w, h) = (want.GetValueOrDefault(column, "<none>"), have.GetValueOrDefault(column, "<none>"));
                if (w != h) yield return $"{table} row {id}.{column}: table {w}, events {h}";
            }
        }
    }

    private static string Json(object? value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var w = new Utf8JsonWriter(buffer, new JsonWriterOptions { SkipValidation = true })) ChangeEventJson.WriteValue(w, value);
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static string Unescaped(JsonElement image) => "{" + string.Join(",", image.EnumerateObject().Select(p =>
        $"\"{p.Name}\":{(p.Value.ValueKind == JsonValueKind.String ? "\"" + p.Value.GetString() + "\"" : p.Value.GetRawText())}")) + "}";

    private static async Task<(int Id, string Incarnation)> IdentityAsync(string master, CancellationToken ct)
    {
        await using var conn = new SqlConnection(master);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("SELECT database_id, CONVERT(varchar(40), create_date, 126) FROM sys.databases WHERE name = @n", conn);
        cmd.Parameters.AddWithValue("@n", DatabaseName);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        await r.ReadAsync(ct);
        return (r.GetInt32(0), r.GetString(1));
    }

    private static async Task ExecAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
        await cmd.ExecuteNonQueryAsync(ct);
    }
}

/// <summary>Remembers which server version a miner build has been verified against (<c>logminer.dbo.self_tests</c>).</summary>
public sealed class SelfTestLog(string connectionString, string serverName)
{
    /// <summary>Identity of this build: any change of the capture code makes the self-test run again.</summary>
    public static string MinerBuild { get; } = string.Create(CultureInfo.InvariantCulture,
        $"{typeof(SelfTest).Assembly.ManifestModule.ModuleVersionId:N}{typeof(DatabaseCapture).Assembly.ManifestModule.ModuleVersionId:N}");

    public async Task EnsureSchemaAsync(CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("""
            IF OBJECT_ID(N'dbo.self_tests', N'U') IS NULL
                CREATE TABLE dbo.self_tests (
                    server_name     sysname        NOT NULL,
                    product_version varchar(60)    NOT NULL,
                    miner_build     char(64)       NOT NULL,
                    passed          bit            NOT NULL,
                    problems        nvarchar(max)  NULL,
                    tested_at       datetime2(3)   NOT NULL CONSTRAINT df_self_tests_at DEFAULT SYSUTCDATETIME(),
                    CONSTRAINT pk_self_tests PRIMARY KEY (server_name, product_version, miner_build));
            """, conn);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task<bool> IsVerifiedAsync(string productVersion, CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("SELECT COUNT(*) FROM dbo.self_tests WHERE server_name = @s AND product_version = @v AND miner_build = @b AND passed = 1", conn);
        Key(cmd, productVersion);
        return (int)(await cmd.ExecuteScalarAsync(ct))! > 0;
    }

    public async Task SaveAsync(SelfTestResult result, CancellationToken ct)
    {
        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync(ct);
        await using var cmd = new SqlCommand("""
            DELETE dbo.self_tests WHERE server_name = @s AND product_version = @v AND miner_build = @b;
            INSERT dbo.self_tests (server_name, product_version, miner_build, passed, problems) VALUES (@s, @v, @b, @p, @problems);
            """, conn);
        Key(cmd, result.ProductVersion);
        cmd.Parameters.AddWithValue("@p", result.Passed);
        cmd.Parameters.Add(new SqlParameter("@problems", System.Data.SqlDbType.NVarChar, -1) { Value = result.Problems.Count == 0 ? DBNull.Value : string.Join('\n', result.Problems) });
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private void Key(SqlCommand cmd, string productVersion)
    {
        cmd.Parameters.AddWithValue("@s", serverName);
        cmd.Parameters.AddWithValue("@v", productVersion);
        cmd.Parameters.AddWithValue("@b", MinerBuild);
    }
}
