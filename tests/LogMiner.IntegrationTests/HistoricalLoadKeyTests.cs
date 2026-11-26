using System.Text.Json;
using LogMiner.Capture;
using LogMiner.Schema;
using LogMiner.SqlServer;
using Microsoft.Data.SqlClient;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>
/// Historical loads of tables with every kind of key: each row is loaded exactly once whatever the chunk size, its values are the same JSON as
/// the stream's, and a change that commits while a chunk is read drops that row of the chunk and no other.
/// </summary>
public class HistoricalLoadKeyTests
{
    public static TheoryData<string, string, string> Tables => new()
    {
        { "time series", "dev int NOT NULL, ts datetime2(7) NOT NULL, PRIMARY KEY (dev, ts)", "SELECT x / 20, DATEADD(nanosecond, 100 * (x % 20), CONVERT(datetime2(7), '2026-01-01T00:00:00')), x FROM n" },
        { "datetime and offset", "a datetime NOT NULL, b datetimeoffset(3) NOT NULL, PRIMARY KEY (b, a)", "SELECT DATEADD(millisecond, 10 * (x % 7), CONVERT(datetime, '20260101')), TODATETIMEOFFSET(DATEADD(millisecond, x / 7, CONVERT(datetime2(3), '2026-01-01')), 60 * (x % 3)), x FROM n" },
        { "time and date", "a date NOT NULL, b time(7) NOT NULL, PRIMARY KEY (a, b)", "SELECT DATEADD(day, x / 9, CONVERT(date, '2026-01-01')), DATEADD(nanosecond, 100 * (x % 9), CONVERT(time(7), '12:00')), x FROM n" },
        { "nullable unique clustered", "a int NULL, b int NULL, v int NULL", "SELECT NULLIF(x / 8, 2), NULLIF(x % 8, 3), x, x FROM n" },
        { "cyrillic", "k varchar(20) COLLATE Cyrillic_General_CI_AS NOT NULL PRIMARY KEY", "SELECT NCHAR(0x0430 + x % 32) + NCHAR(0x0431 + x / 32) + N'ключ', x FROM n" },
        { "utf8", "k varchar(40) COLLATE Latin1_General_100_CI_AS_SC_UTF8 NOT NULL PRIMARY KEY", "SELECT NCHAR(0x4E00 + x * 7) + N'å', x FROM n" },
        { "japanese", "k varchar(20) COLLATE Japanese_CI_AS NOT NULL PRIMARY KEY", "SELECT NCHAR(0x30A2 + x % 60) + CONVERT(nvarchar(5), x) + N'漢', x FROM n" },
        { "turkish", "k varchar(20) COLLATE Turkish_CI_AS NOT NULL PRIMARY KEY", "SELECT CHAR(97 + x % 26) + CASE x % 3 WHEN 0 THEN N'ı' WHEN 1 THEN N'i' ELSE N'ş' END + CONVERT(nvarchar(5), x), x FROM n" },
        { "char", "k char(12) COLLATE Cyrillic_General_CI_AS NOT NULL, l varchar(5) NOT NULL, PRIMARY KEY (k, l)", "SELECT NCHAR(0x0430 + x % 32), CONVERT(varchar(5), x), x FROM n" },
        { "half surrogate pairs", "k nvarchar(10) COLLATE Latin1_General_BIN2 NOT NULL PRIMARY KEY", "SELECT NCHAR(0xD7F0 + x * 37 % 2300) + NCHAR(0x0041 + x % 5) + CONVERT(nvarchar(5), x), x FROM n" },
        { "nvarchar", "k nvarchar(30) COLLATE Latin1_General_100_CS_AS_SC NOT NULL, j nchar(4) NOT NULL, PRIMARY KEY (j, k)", "SELECT NCHAR(0xC0 + x) + CHAR(9) + CHAR(31) + CONVERT(nvarchar(5), x), CONVERT(nchar(4), x % 3), x FROM n" },
        { "decimal", "k decimal(38,10) NOT NULL, l numeric(5,0) NOT NULL, PRIMARY KEY (k DESC, l)", "SELECT CONVERT(decimal(38,10), '1234567890123456789012345678.0') + x / 10.0, x, x FROM n" },
        { "guid", "k uniqueidentifier NOT NULL PRIMARY KEY", "SELECT NEWID(), x FROM n" },
        { "binary", "k varbinary(16) NOT NULL, l binary(3) NOT NULL, PRIMARY KEY (k, l)", "SELECT CONVERT(varbinary(16), x % 5), CONVERT(binary(3), x), x FROM n" },
        { "float and money", "k float NOT NULL, l money NOT NULL, m bit NOT NULL, PRIMARY KEY (k, l, m)", "SELECT SQRT(x / 4), x / 2 * 0.0001, x % 2, x FROM n" },
        { "clr and variant", "id int NOT NULL PRIMARY KEY, h hierarchyid NULL, g geography NULL, s sql_variant NULL, t text NULL", "SELECT x, CONVERT(hierarchyid, '/' + CONVERT(varchar(5), x) + '/'), geography::Point(x % 90, x % 180, 4326), CASE WHEN x % 2 = 0 THEN x END, NULL, x FROM n" },
    };

    /// <summary>Lets something commit while the chunk is being read: after its rows, before its end.</summary>
    private sealed class DuringRead(ISnapshotSource inner, Func<SnapshotChunk, Task> during, Func<Task<Lsn>> end) : ISnapshotSource
    {
        public async Task<SnapshotChunk> ReadChunkAsync(TableLayout table, object?[]? afterKey, int chunkSize, CancellationToken ct)
        {
            var chunk = await inner.ReadChunkAsync(table, afterKey, chunkSize, ct);
            await during(chunk);
            return new SnapshotChunk { Table = chunk.Table, Low = chunk.Low, High = await end(), Rows = chunk.Rows, LastKey = chunk.LastKey };
        }
    }

    [Theory]
    [MemberData(nameof(Tables))]
    public async Task Every_row_is_loaded_once_with_the_streams_values(string what, string columns, string rows)
    {
        var nullable = what == "nullable unique clustered";
        await using var h = await CaptureHarness.CreateAsync($"CREATE TABLE dbo.t ({columns}, n int NOT NULL);", ["t"],
            options: "", after: nullable ? "CREATE UNIQUE CLUSTERED INDEX cx ON dbo.t (a, b);" : "");
        await h.SqlAsync($"WITH n AS (SELECT TOP (100) CONVERT(int, ROW_NUMBER() OVER (ORDER BY (SELECT 1))) AS x FROM sys.all_columns) INSERT dbo.t {(nullable ? "SELECT NULLIF(x / 8, 2), NULLIF(x % 8, 3), x, x FROM n" : rows)};");
        await h.PumpAllAsync();
        await h.CheckpointAsync();
        var stream = h.Events().ToDictionary(e => e.GetProperty("after").GetProperty("n").GetInt32(), e => e.GetProperty("after").GetRawText());
        Assert.Equal(100, stream.Count);
        var layout = h.Capture.Schema!.Tables.First(l => l.Table == "t");
        Assert.NotEmpty(layout.Key);
        foreach (var chunk in new[] { 1, 2, 7, 99, 100, 101 })
        {
            var read = new HashSet<int>();
            var source = new SqlSnapshotSource(TestServer.ConnectionString());
            object?[]? after = null;
            for (var chunks = 0; ; chunks++)
            {
                Assert.True(chunks <= 100 / chunk + 2, $"chunk {chunk}: the load does not end");
                var c = await source.ReadChunkAsync(layout, after, chunk, CancellationToken.None);
                foreach (var r in c.Rows) Assert.True(read.Add((int)r["n"]!), $"chunk {chunk}: row {r["n"]} came twice");
                if (c.LastKey is null || c.Rows.Count < chunk) break;
                after = c.LastKey;
            }
            Assert.Equal(stream.Keys.Order(), read.Order());
        }
        // through the pipeline: the same JSON, and a row that changes while its chunk is read takes no other row with it
        var load = new TableSnapshot(layout, "load", 30);
        h.Capture.RequestSnapshot(load);
        var changed = new HashSet<int>();
        var during = new DuringRead(new SqlSnapshotSource(TestServer.ConnectionString()), async c =>
        {
            if (c.Rows.Count < 5 || changed.Count >= 3) return;
            var n = (int)c.Rows[c.Rows.Count / 2]["n"]!;
            changed.Add(n);
            await h.SqlAsync($"UPDATE dbo.t SET n = -n WHERE n = {n}; UPDATE dbo.t SET n = -n WHERE n = -{n};");
        }, async () =>
        {
            await using var conn = new SqlConnection(TestServer.ConnectionString());
            await conn.OpenAsync();
            await using var cmd = new SqlCommand("SELECT log_end_lsn FROM sys.dm_db_log_stats(DB_ID(@d))", conn);
            cmd.Parameters.AddWithValue("@d", h.Db);
            return Lsn.Parse((string)(await cmd.ExecuteScalarAsync())!);
        });
        for (var i = 0; i < 50 && !load.Done; i++) await h.PumpOnceAsync(snapshots: during);
        Assert.True(load.Done && !load.Failed, load.Error);
        await h.PumpAllAsync();
        var loaded = h.Events().Where(e => e.GetProperty("op").GetString() == "r").ToDictionary(e => e.GetProperty("after").GetProperty("n").GetInt32(), e => e.GetProperty("after"));
        Assert.True(changed.Count > 0);
        Assert.Equal(stream.Keys.Where(n => !changed.Contains(n)).Order(), loaded.Keys.Order());
        foreach (var (n, image) in loaded)
        {
            using var doc = JsonDocument.Parse(stream[n]);
            foreach (var p in image.EnumerateObject()) // sql_variant and text are not decoded from the log
                if (what != "clr and variant" || p.Name is "id" or "h" or "g" or "n") Assert.Equal(doc.RootElement.GetProperty(p.Name).GetRawText(), p.Value.GetRawText());
        }
    }

    /// <summary>The stream has every row; a load that only sees some must not look complete.</summary>
    [Fact]
    public async Task A_load_under_row_level_security_is_refused()
    {
        await using var h = await CaptureHarness.CreateAsync("CREATE TABLE dbo.t (id int NOT NULL PRIMARY KEY, v int NULL);", ["t"]);
        await h.SqlAsync("INSERT dbo.t VALUES (1, 1), (2, 2), (3, 3), (4, 4);");
        await h.PumpAllAsync();
        var source = new SqlSnapshotSource(TestServer.ConnectionString());
        var layout = h.Capture.Schema!.Tables.First(l => l.Table == "t");
        Assert.Equal(4, (await source.ReadChunkAsync(layout, null, 10, CancellationToken.None)).Rows.Count);
        await h.SqlAsync("CREATE FUNCTION dbo.f(@id int) RETURNS TABLE WITH SCHEMABINDING AS RETURN SELECT 1 AS ok WHERE @id < 3");
        await h.SqlAsync("CREATE SECURITY POLICY dbo.p ADD FILTER PREDICATE dbo.f(id) ON dbo.t");
        var refused = await Assert.ThrowsAsync<InvalidOperationException>(() => source.ReadChunkAsync(layout, null, 10, CancellationToken.None));
        Assert.Contains("row-level security", refused.Message, StringComparison.Ordinal);
    }
}
