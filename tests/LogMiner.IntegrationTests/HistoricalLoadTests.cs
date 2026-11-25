using System.Diagnostics;
using System.Text.Json;
using LogMiner.Capture;
using LogMiner.Pipeline;
using LogMiner.Sinks;
using LogMiner.SqlServer;
using Microsoft.Data.SqlClient;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>
/// Historical load against the real engine while writers keep committing: the capture starts "Latest" over existing rows (so only
/// the load can deliver them), small chunks make commits land inside chunk windows [L, H), and a replica built from every event
/// (stream changes and snapshot rows, in delivery order) must equal the table at the end - nothing missing, nothing stale.
/// </summary>
public class HistoricalLoadTests
{
    private const string Ddl = "CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NOT NULL, pad nvarchar(40) NULL);";

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(17)]
    public async Task Load_under_concurrent_writes_converges_to_the_table(int chunkSize)
    {
        var db = await TestServer.CreateDatabaseAsync(Ddl, "t");
        try
        {
            await TestServer.ExecAsync("INSERT dbo.t (id, v, pad) SELECT TOP (300) ROW_NUMBER() OVER (ORDER BY (SELECT 1)), 0, N'history' FROM sys.all_objects;", db);
            var (dbId, incarnation) = await TestServer.IdentityAsync(db);
            var capture = new DatabaseCapture(dbId, db, incarnation, null, StartPosition.Latest);
            var store = new MemoryCheckpointStore();
            var sink = new MemorySink();
            var encoder = new EventEncoder(new EncoderOptions { ServerName = "it" });
            var settings = new CaptureSettings { MaxRowsPerRead = 200 };
            await using var reader = new SqlConnection(TestServer.ConnectionString());
            await reader.OpenAsync();
            var source = new EngineLogSource(reader, TestServer.ConnectionString(), CaptureScope.CdcTrackedTables);
            var snapshots = new SqlSnapshotSource(TestServer.ConnectionString(), lockTimeoutMs: 2000);

            async Task PumpAsync()
            {
                await capture.PumpAsync(source, encoder, sink, settings, CancellationToken.None, snapshots);
                await CheckpointCoordinator.FlushAsync([capture], store, null, TimeSpan.Zero, null, CancellationToken.None);
            }

            // go live (the "Latest" start point is durable) before anything else happens
            var sw = Stopwatch.StartNew();
            while ((!capture.Live || capture.HoldingForBaseline || capture.Schema is null) && sw.Elapsed < TimeSpan.FromSeconds(30)) await PumpAsync();
            Assert.True(capture.Live && !capture.HoldingForBaseline, "capture did not go live");

            using var stop = new CancellationTokenSource();
            var writers = Enumerable.Range(0, 3).Select(w => Task.Run(() => WriteAsync(db, w, stop.Token))).ToArray();
            await Task.Delay(200);
            var layout = capture.Schema!.Tables.First(l => l.Table == "t");
            var load = new TableSnapshot(layout, "load", chunkSize) { RetryDelay = TimeSpan.FromMilliseconds(10) };
            capture.RequestSnapshot(load);
            while (!load.Done && sw.Elapsed < TimeSpan.FromSeconds(120)) await PumpAsync();
            await stop.CancelAsync();
            var ops = (await Task.WhenAll(writers)).Sum();
            Assert.True(load.Done, "historical load did not finish");
            Assert.False(load.Failed, load.Error);

            // drain the stream
            for (var i = 0; i < 50; i++) await PumpAsync();

            var replica = new SortedDictionary<int, (int V, string? Pad)>();
            var seen = new HashSet<string>();
            int reads = 0, changes = 0;
            foreach (var m in sink.Snapshot())
            {
                if (!seen.Add(m.MessageId)) continue;
                using var doc = JsonDocument.Parse(m.Payload);
                var root = doc.RootElement;
                var op = root.GetProperty("op").GetString();
                if (op == "r") reads++; else changes++;
                var image = root.GetProperty(op == "d" ? "before" : "after");
                var id = image.GetProperty("id").GetInt32();
                if (op == "d") replica.Remove(id);
                else replica[id] = (image.GetProperty("v").GetInt32(), image.GetProperty("pad").ValueKind == JsonValueKind.Null ? null : image.GetProperty("pad").GetString());
            }

            var table = new SortedDictionary<int, (int V, string? Pad)>();
            await using (var conn = new SqlConnection(TestServer.ConnectionString(db)))
            {
                await conn.OpenAsync();
                await using var cmd = new SqlCommand("SELECT id, v, pad FROM dbo.t", conn);
                await using var r = await cmd.ExecuteReaderAsync();
                while (await r.ReadAsync()) table[r.GetInt32(0)] = (r.GetInt32(1), r.IsDBNull(2) ? null : r.GetString(2));
            }
            Assert.True(ops > 20, $"writers committed only {ops} changes"); // the load is over soon; on a slow disk there were 38
            Assert.True(reads > 0 && changes > 0, $"reads={reads} changes={changes}");
            var diff = table.Keys.Union(replica.Keys)
                .Where(k => !table.TryGetValue(k, out var t) || !replica.TryGetValue(k, out var e) || t != e)
                .Take(5).Select(k => $"{k}: table={(table.TryGetValue(k, out var t) ? t.ToString() : "-")} events={(replica.TryGetValue(k, out var e) ? e.ToString() : "-")}")
                .ToList();
            Assert.True(diff.Count == 0, $"replica differs from the table (chunk {chunkSize}, {ops} concurrent changes, {reads} snapshot rows): {string.Join("; ", diff)}");
        }
        finally { await TestServer.DropDatabaseAsync(db); }
    }

    /// <summary>Random autocommit inserts, updates and deletes until stopped; returns the number of committed changes.</summary>
    private static async Task<int> WriteAsync(string db, int writer, CancellationToken stop)
    {
        var rng = new Random(writer * 7919 + 17);
        var nextId = 1_000_000 * (writer + 1);
        var ops = 0;
        await using var conn = new SqlConnection(TestServer.ConnectionString(db));
        await conn.OpenAsync(CancellationToken.None);
        while (!stop.IsCancellationRequested)
        {
            var roll = rng.Next(10);
            var sql = roll switch
            {
                < 6 => "UPDATE dbo.t SET v = v + 1, pad = CONCAT(N'w', @w, N'-', v + 1) WHERE id = (SELECT TOP (1) id FROM dbo.t WHERE id >= @k ORDER BY id);",
                < 8 => "INSERT dbo.t (id, v, pad) VALUES (@n, 1, NULL);",
                _ => "DELETE dbo.t WHERE id = (SELECT TOP (1) id FROM dbo.t WHERE id >= @k ORDER BY id);",
            };
            await using var cmd = new SqlCommand(sql, conn);
            cmd.Parameters.AddWithValue("@w", writer);
            cmd.Parameters.AddWithValue("@k", rng.Next(1, 320));
            cmd.Parameters.AddWithValue("@n", nextId++);
            try { ops += await cmd.ExecuteNonQueryAsync(CancellationToken.None); }
            catch (SqlException ex) when (ex.Number is 1205 or 2627) { } // deadlock victim / duplicate: just go on
        }
        return ops;
    }
}
