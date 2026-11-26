using System.Buffers;
using System.Text;
using System.Text.Json;
using LogMiner.Capture;
using LogMiner.Events;
using LogMiner.Pipeline;
using LogMiner.Schema;
using LogMiner.Sinks;
using LogMiner.SqlServer;
using Microsoft.Data.SqlClient;

namespace LogMiner.IntegrationTests;

/// <summary>
/// One captured database on the real engine, driven step by step. The oracle is the table itself: its rows are read with
/// <see cref="SqlSnapshotSource"/> (proven byte-identical to stream images by the round-trip test) and compared, column by column as
/// JSON, with a replica built only from published events.
/// </summary>
internal sealed class CaptureHarness : IAsyncDisposable
{
    private readonly SqlConnection _reader = new(TestServer.ConnectionString());
    private readonly CaptureSettings _settings;
    private readonly int _dbId;
    private readonly string _incarnation;

    public string Db { get; }
    public MemorySink Sink { get; } = new();
    public MemoryCheckpointStore Store { get; } = new();
    public DatabaseCapture Capture { get; private set; }
    public List<CaptureGap> Gaps { get; } = [];
    public EventEncoder Encoder { get; } = new(new EncoderOptions { ServerName = "it" });
    public SqlLogReleaser Releaser { get; } = new(TestServer.ConnectionString());

    private CaptureHarness(string db, int dbId, string incarnation, int maxRows)
    {
        Db = db;
        _dbId = dbId;
        _incarnation = incarnation;
        _settings = new CaptureSettings { StartPosition = StartPosition.Earliest, MaxRowsPerRead = maxRows };
        Capture = new DatabaseCapture(dbId, db, incarnation, null, StartPosition.Earliest);
    }

    public static async Task<CaptureHarness> CreateAsync(string ddl, string[] tables, int maxRows = 20_000, string options = "", string after = "")
    {
        var db = await TestServer.CreateDatabaseAsync(options + ddl + after, tables);
        var (id, incarnation) = await TestServer.IdentityAsync(db);
        var h = new CaptureHarness(db, id, incarnation, maxRows);
        await h._reader.OpenAsync();
        return h;
    }

    public Task SqlAsync(string sql) => TestServer.ExecAsync(sql, Db);

    /// <summary>One expected event: exactly what the row looked like (with the columns it had) right before and right after the statement.</summary>
    public sealed record Expected(string Table, string Op, string Key, Dictionary<string, string>? Before, Dictionary<string, string>? After);
    public List<Expected> Journal { get; } = [];

    /// <summary>DDL (or anything else that is not expected to publish events).</summary>
    public async Task DdlAsync(string sql) => await SqlAsync(sql);

    /// <summary>A statement that changes exactly one row; its before and after images are read from the table and journalled.</summary>
    public async Task DmlAsync(string op, int id, string sql, string table = "t")
    {
        var key = id.ToString(System.Globalization.CultureInfo.InvariantCulture);
        var before = op == "c" ? null : (await TableAsync(table))[key];
        await SqlAsync(sql);
        Journal.Add(new Expected(table, op, key, before, op == "d" ? null : (await TableAsync(table))[key]));
    }

    /// <summary>First differences between the journal and the published events (empty = every change delivered once, in order, exactly).</summary>
    public List<string> JournalDiff()
    {
        var diffs = new List<string>();
        var events = Events().ToList();
        if (events.Count != Journal.Count) diffs.Add($"{events.Count} events for {Journal.Count} changes");
        for (var i = 0; i < Math.Min(events.Count, Journal.Count) && diffs.Count < 12; i++)
        {
            var (e, x) = (events[i], Journal[i]);
            var image = e.GetProperty(x.Op == "d" ? "before" : "after");
            var what = $"#{i} {x.Op} {x.Table}/{x.Key}";
            if (e.GetProperty("source").GetProperty("table").GetString() != x.Table || e.GetProperty("op").GetString() != x.Op || image.ValueKind != JsonValueKind.Object || image.GetProperty("id").GetRawText() != x.Key)
            {
                diffs.Add($"{what}: got {e.GetProperty("op").GetString()} {e.GetProperty("source").GetProperty("table").GetString()}/{(image.ValueKind == JsonValueKind.Object ? image.GetProperty("id").GetRawText() : "-")}");
                break;
            }
            Compare(x.Before, e.GetProperty("before"), what + " before", diffs);
            Compare(x.After, e.GetProperty("after"), what + " after", diffs);
        }
        return diffs;
    }

    private static void Compare(Dictionary<string, string>? expected, JsonElement got, string what, List<string> diffs)
    {
        if (expected is null) { if (got.ValueKind != JsonValueKind.Null) diffs.Add($"{what}: unexpected image"); return; }
        var actual = got.ValueKind == JsonValueKind.Object ? got.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetRawText()) : [];
        foreach (var column in expected.Keys.Union(actual.Keys))
        {
            var want = expected.GetValueOrDefault(column, "<no column>");
            var have = actual.GetValueOrDefault(column, "<no column>");
            if (want != have) diffs.Add($"{what}.{column}: row={want} event={have}");
        }
    }

    public async Task<bool> PumpOnceAsync(ISink? sink = null, ISnapshotSource? snapshots = null)
    {
        var source = new EngineLogSource(_reader, TestServer.ConnectionString(), CaptureScope.CdcTrackedTables);
        return await Capture.PumpAsync(source, Encoder, sink ?? Sink, _settings, CancellationToken.None, snapshots, new GapRecorder(Gaps));
    }

    public async Task PumpAllAsync() { while (await PumpOnceAsync()) { } }

    /// <summary>Durable checkpoint; with <paramref name="release"/> also sp_repldone and a log backup (so SQL Server truncates behind it).</summary>
    public async Task CheckpointAsync(bool release = false)
    {
        await CheckpointCoordinator.FlushAsync([Capture], Store, release ? Releaser : null, TimeSpan.Zero, (_, ex) => throw ex, CancellationToken.None);
        if (release) await TestServer.ExecAsync($"BACKUP LOG [{Db}] TO DISK = 'NUL';");
    }

    /// <summary>The process dies and restarts: everything in memory is lost, the capture resumes from its durable checkpoint.</summary>
    public async Task RestartAsync() =>
        Capture = new DatabaseCapture(_dbId, Db, _incarnation, await Store.LoadAsync(Db, CancellationToken.None), StartPosition.Earliest);

    private sealed class GapRecorder(List<CaptureGap> gaps) : IGapLog
    {
        public Task RecordAsync(CaptureGap gap, bool resync, CancellationToken ct) { gaps.Add(gap); return Task.CompletedTask; }
    }

    public IEnumerable<JsonElement> Events()
    {
        var seen = new HashSet<string>();
        foreach (var m in Sink.Snapshot())
        {
            if (!seen.Add(m.MessageId)) continue;
            using var doc = JsonDocument.Parse(m.Payload);
            if (doc.RootElement.GetProperty("op").GetString() != "l") yield return doc.RootElement.Clone();
        }
    }

    /// <summary>key → (column → JSON) after applying every published event of a table in order.</summary>
    public Dictionary<string, Dictionary<string, string>> Replica(string table)
    {
        var rows = new Dictionary<string, Dictionary<string, string>>();
        foreach (var e in Events().Where(e => e.GetProperty("source").GetProperty("table").GetString() == table))
        {
            var op = e.GetProperty("op").GetString();
            var image = e.GetProperty(op == "d" ? "before" : "after");
            var key = image.GetProperty("id").GetRawText();
            if (op == "d") rows.Remove(key);
            else rows[key] = image.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetRawText());
        }
        return rows;
    }

    public async Task<Dictionary<string, Dictionary<string, string>>> TableAsync(string table)
    {
        await using var conn = new SqlConnection(TestServer.ConnectionString());
        await conn.OpenAsync();
        // only the current layouts are needed: no history before the miner's own position
        var schema = await SchemaLoader.LoadAsync(conn, Capture.Database, CaptureScope.CdcTrackedTables, Capture.LastLsn.IsZero ? null : Capture.LastLsn, new HashSet<long>(), CancellationToken.None);
        var layout = schema.Tables.First(l => l.Table == table);
        var rows = new Dictionary<string, Dictionary<string, string>>();
        if (layout.Key.Count == 0)
        {
            // heap: the snapshot source needs a key, so declare one for the read (tests always use "id")
            layout = new TableLayout(layout.DatabaseId, layout.Database, layout.ObjectId, layout.Schema, layout.Table, layout.PartitionId, layout.Compression,
                layout.Columns.Select(c => c.Name == "id" ? c with { KeyOrdinal = 1 } : c).ToArray());
        }
        var chunk = await new SqlSnapshotSource(TestServer.ConnectionString()).ReadChunkAsync(layout, null, 1_000_000, CancellationToken.None);
        foreach (var r in chunk.Rows)
        {
            var row = new Dictionary<string, string>();
            for (var i = 0; i < layout.Visible.Count; i++) row[layout.Visible[i].Name] = Json(r.Values[i]);
            rows[row["id"]] = row;
        }
        return rows;
    }

    private static string Json(object? value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var w = new Utf8JsonWriter(buffer, new JsonWriterOptions { SkipValidation = true })) ChangeEventJson.WriteValue(w, value);
        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    /// <summary>Differences between the replica and the table (empty = identical).</summary>
    public async Task<List<string>> DiffAsync(string table)
    {
        var replica = Replica(table);
        var actual = await TableAsync(table);
        var diffs = new List<string>();
        foreach (var key in actual.Keys.Union(replica.Keys).Order(StringComparer.Ordinal))
        {
            if (!replica.TryGetValue(key, out var r)) { diffs.Add($"row {key}: in the table, MISSING from the events"); continue; }
            if (!actual.TryGetValue(key, out var a)) { diffs.Add($"row {key}: in the events, not in the table"); continue; }
            foreach (var (col, value) in a)
                if (!r.TryGetValue(col, out var got)) diffs.Add($"row {key}.{col}: table={value} events=<no such column>");
                else if (got != value) diffs.Add($"row {key}.{col}: table={value} events={got}");
        }
        return diffs;
    }

    public async ValueTask DisposeAsync()
    {
        await _reader.DisposeAsync();
        await Releaser.DisposeAsync();
        await TestServer.DropDatabaseAsync(Db);
    }
}
