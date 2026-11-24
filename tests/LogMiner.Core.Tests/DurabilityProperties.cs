using System.Buffers;
using System.Text.Json;
using Hegel;
using Hegel.Xunit;
using LogMiner.Pipeline;
using LogMiner.Sinks;
using LogMiner.Tests;
using Xunit;

namespace LogMiner.Core.Tests;

/// <summary>Files that hold what must not be lost: spool segments, checkpoints, event files - written, damaged, read again.</summary>
public sealed class DurabilityProperties : IDisposable
{
    private readonly List<string> _directories = [];

    private string NewDirectory()
    {
        var d = Path.Combine(Path.GetTempPath(), "lm-dur-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(d);
        _directories.Add(d);
        return d;
    }

    public void Dispose()
    {
        foreach (var d in _directories) try { Directory.Delete(d, recursive: true); } catch (IOException) { }
    }

    private static SinkMessage Message(TestCase tc, int n)
    {
        var texts = Generators.Text().MaxSize(12);
        return new SinkMessage
        {
            Database = tc.Draw(Generators.Booleans(), "hasDb") ? tc.Draw(texts, "db") : null,
            Stream = tc.Draw(Generators.Booleans(), "hasStream") ? tc.Draw(texts, "stream") : null,
            Position = tc.Draw(Generators.Booleans(), "hasPosition") ? new SourcePosition(new Lsn((uint)tc.Draw(Generators.Integers().Min(0), "vlf"), 2, 3), tc.Draw(Generators.Integers(), "seq")) : null,
            Destination = tc.Draw(texts, "destination"), Key = tc.Draw(texts, "key"), MessageId = $"{n}:{tc.Draw(texts, "id")}",
            ContentType = tc.Draw(Generators.Booleans(), "hasType") ? tc.Draw(texts, "type") : null,
            Headers = tc.Draw(Generators.Lists(texts).MaxSize(3), "headers").Select((h, i) => new KeyValuePair<string, string>("h" + i, h)).ToArray(),
            Payload = tc.Draw(Generators.Binary().MaxSize(64), "payload"),
        };
    }

    [HegelTest(TestCases = 2000)]
    public void A_spool_record_reads_back_as_it_was_written(TestCase tc)
    {
        var m = Message(tc, 1);
        var r = new SpoolRecord(tc.Draw(Generators.Longs(), "seq"), tc.Draw(Generators.Longs(), "ms"), m, tc.Draw(Generators.Longs(), "batch"));
        var buffer = new ArrayBufferWriter<byte>();
        SpoolCodec.Write(r, buffer);
        var tail = tc.Draw(Generators.Binary().MaxSize(20), "tail");
        Assert.True(SpoolCodec.TryRead([.. buffer.WrittenSpan, .. tail], out var got, out var consumed));
        Assert.Equal(buffer.WrittenCount, consumed);
        Assert.Equal((r.Seq, r.AppendedUnixMs, r.Batch), (got!.Seq, got.AppendedUnixMs, got.Batch));
        var g = got.Message;
        Assert.Equal((m.Database, m.Destination, m.Key, m.MessageId, m.ContentType), (g.Database, g.Destination, g.Key, g.MessageId, g.ContentType));
        if (m.Stream is not null && m.Position is not null) Assert.Equal((m.Stream, m.Position), (g.Stream, g.Position)); // a position counts within a stream only
        Assert.Equal(m.Headers, g.Headers);
        Assert.Equal(m.Payload.ToArray(), g.Payload.ToArray());
        // any single damaged bit, any cut: not a record
        var bytes = buffer.WrittenSpan.ToArray();
        var at = tc.Draw(Generators.Integers().Min(0).Max(bytes.Length - 1), "at");
        Assert.False(SpoolCodec.TryRead(bytes.AsSpan(0, at), out _, out _));
        bytes[at] ^= (byte)(1 << tc.Draw(Generators.Integers().Min(0).Max(7), "bit"));
        Assert.False(SpoolCodec.TryRead(bytes, out _, out _));
    }

    [Fact]
    public void A_message_that_could_not_be_read_back_is_refused()
    {
        var m = new SinkMessage { Destination = "d", Key = "k", MessageId = "big", Payload = new byte[SpoolCodec.MaxBodySize] };
        Assert.NotNull(SpoolCodec.Problem(m));
        Assert.Throws<ArgumentException>(() => SpoolCodec.Write(new SpoolRecord(1, 1, m), new ArrayBufferWriter<byte>()));
        var fits = new SinkMessage { Destination = "d", Key = "k", MessageId = "big", Payload = new byte[SpoolCodec.MaxBodySize - 1024] };
        var buffer = new ArrayBufferWriter<byte>();
        SpoolCodec.Write(new SpoolRecord(1, 1, fits), buffer);
        Assert.True(SpoolCodec.TryRead(buffer.WrittenSpan, out _, out _));
    }

    /// <summary>A destination that takes the first messages and then answers no more.</summary>
    private sealed class Stopping(int takes) : ISink
    {
        private readonly Lock _gate = new();
        public List<string> Ids { get; } = [];
        public string Name => "stopping";
        public SinkHealth Health => new(Name, true);
        public Task StartAsync(CancellationToken ct) => Task.CompletedTask;
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
        public int Count { get { lock (_gate) return Ids.Count; } }
        public ValueTask<Task> SendAsync(IReadOnlyList<SinkMessage> messages, CancellationToken ct)
        {
            lock (_gate)
            {
                if (Ids.Count + messages.Count > takes) return ValueTask.FromResult<Task>(new TaskCompletionSource().Task);
                Ids.AddRange(messages.Select(m => m.MessageId));
                return ValueTask.FromResult(Task.CompletedTask);
            }
        }
    }

    /// <summary>
    /// Every message was acknowledged because it was on disk. Then something happens to the files. Either the spool still delivers everything
    /// that was not delivered, or it refuses to start: it never goes on without saying so. (Damage up to the very end looks like a write that was
    /// interrupted: only what is damaged may then be missing.)
    /// </summary>
    [HegelTest(TestCases = 150)]
    public void Damage_to_the_spool_is_never_passed_over(TestCase tc)
    {
        var dir = NewDirectory();
        var n = tc.Draw(Generators.Integers().Min(2).Max(30), "messages");
        var taken = tc.Draw(Generators.Integers().Min(0).Max(n), "delivered");
        var options = new SpoolOptions { Directory = dir, AckOn = SpoolAck.Spooled, SegmentBytes = tc.Draw(Generators.SampledFrom(new[] { 1L, 400L, 900L, 100_000L }), "segmentBytes"), ForwardBatchMessages = 1, CursorFlushInterval = TimeSpan.FromMilliseconds(1), RetryMin = TimeSpan.FromMilliseconds(1) };
        var first = new Stopping(taken);
        Blocking.Run(async () =>
        {
            var spool = new SpoolSink(first, options);
            await spool.StartAsync(CancellationToken.None);
            for (var i = 0; i < n; i++) await await spool.SendAsync([new SinkMessage { Destination = "d", Key = "k", MessageId = "m" + i, Payload = new byte[100] }], CancellationToken.None);
            for (var i = 0; i < 2000 && first.Count < taken; i++) await Task.Delay(1);
            for (var i = 0; i < 2000 && spool.DeliveredSeq < taken; i++) await Task.Delay(1);
            await spool.SimulateCrashAsync();
        });
        var files = Directory.GetFiles(dir, "*.seg").Order(StringComparer.Ordinal).ToList();
        var what = tc.Draw(Generators.SampledFrom(new[] { "nothing", "flip", "delete", "cut", "zero", "append", "rename", "cursor" }), "damage");
        var file = files.Count == 0 ? null : tc.Draw(Generators.SampledFrom(files), "file");
        if (file is null && what is not "cursor") what = "nothing";
        var mayBeLost = 0;
        switch (what)
        {
            case "flip" or "zero" or "cut":
            {
                var data = File.ReadAllBytes(file!);
                if (data.Length == 0) break;
                var at = tc.Draw(Generators.Integers().Min(0).Max(data.Length - 1), "at");
                var zeros = what == "zero" ? tc.Draw(Generators.Integers().Min(1).Max(300), "zeros") : 1;
                // Each record was on disk before the next was written. What is damaged up to the end of the last segment looks like a write that
                // was interrupted; anything before a record that is whole does not.
                var starts = new List<int>();
                for (var p = 0; SpoolCodec.TryRead(data.AsSpan(p), out _, out var size); p += size) starts.Add(p);
                var firstDamaged = starts.FindLastIndex(s => s <= at);
                if (file == files[^1] && (what == "cut" || at + zeros > starts[^1])) mayBeLost = starts.Count - firstDamaged;
                if (what == "flip") data[at] ^= (byte)(1 << tc.Draw(Generators.Integers().Min(0).Max(7), "bit"));
                else if (what == "zero") data.AsSpan(at, Math.Min(data.Length - at, zeros)).Clear();
                else data = data[..at];
                File.WriteAllBytes(file!, data);
                break;
            }
            case "delete":
                if (file == files[^1]) mayBeLost = n; // as if it had never been written
                File.Delete(file!);
                break;
            case "append": File.AppendAllText(file!, "garbage"); break;
            case "rename": File.Move(file!, Path.Combine(dir, 99999.ToString("D20", System.Globalization.CultureInfo.InvariantCulture) + ".seg")); break;
            case "cursor": File.WriteAllBytes(Path.Combine(dir, "cursor.lmc"), [1, 2, 3]); break;
        }
        var second = new Stopping(int.MaxValue);
        var started = Blocking.Run(async () =>
        {
            var spool = new SpoolSink(second, options);
            try { await spool.StartAsync(CancellationToken.None); }
            catch (SpoolCorruptedException) { await spool.DisposeAsync(); return false; }
            for (var i = 0; i < 2000 && spool.DeliveredSeq < spool.LastSeq; i++) await Task.Delay(1);
            await spool.DisposeAsync();
            return true;
        });
        if (!started) { Assert.NotEqual("nothing", what); Assert.NotEqual("append", what == "append" && file == files[^1] ? "append" : ""); return; }
        var got = second.Ids.ToHashSet();
        var missing = Enumerable.Range(0, n).Where(i => i >= first.Count && !got.Contains("m" + i)).ToList();
        Assert.True(missing.Count <= mayBeLost, $"{what}: {string.Join(",", missing)} of {n} lost silently ({first.Count} delivered before)");
        if (what is "nothing" or "append" or "cursor") Assert.Empty(missing);
        Assert.Equal(second.Ids.Select(id => int.Parse(id[1..], System.Globalization.CultureInfo.InvariantCulture)).Order(), second.Ids.Select(id => int.Parse(id[1..], System.Globalization.CultureInfo.InvariantCulture)));
    }

    /// <summary>In Delivered mode nothing is acknowledged that is only in the spool: what an earlier run left there is the source's to send.</summary>
    [Fact]
    public void What_was_not_acknowledged_is_not_sent_later()
    {
        var dir = NewDirectory();
        var options = new SpoolOptions { Directory = dir, AckOn = SpoolAck.Delivered };
        Blocking.Run(async () =>
        {
            var spool = new SpoolSink(new Stopping(0), options);
            await spool.StartAsync(CancellationToken.None);
            _ = await spool.SendAsync([new SinkMessage { Destination = "d", Key = "k", MessageId = "old", Payload = new byte[10] }], CancellationToken.None);
            for (var i = 0; i < 1000 && spool.DurableSeq < 1; i++) await Task.Delay(1);
            await spool.SimulateCrashAsync();
            Assert.False(SpoolSink.HasUndelivered(dir));
            var second = new Stopping(int.MaxValue);
            spool = new SpoolSink(second, options);
            await spool.StartAsync(CancellationToken.None);
            await await spool.SendAsync([new SinkMessage { Destination = "d", Key = "k", MessageId = "new", Payload = new byte[10] }], CancellationToken.None);
            await spool.DisposeAsync();
            Assert.Equal(["new"], second.Ids);
        });
    }

    [Fact]
    public void Acknowledged_but_undelivered_records_are_noticed_when_the_spool_is_not_used_any_more()
    {
        var dir = NewDirectory();
        Assert.False(SpoolSink.HasUndelivered(dir));
        Blocking.Run(async () =>
        {
            var options = new SpoolOptions { Directory = dir, AckOn = SpoolAck.Spooled };
            var spool = new SpoolSink(new Stopping(0), options);
            await spool.StartAsync(CancellationToken.None);
            await Assert.ThrowsAnyAsync<IOException>(() => new SpoolSink(new Stopping(0), options).StartAsync(CancellationToken.None)); // one process per directory
            await await spool.SendAsync([new SinkMessage { Destination = "d", Key = "k", MessageId = "a", Payload = new byte[10] }], CancellationToken.None);
            await spool.SimulateCrashAsync();
            Assert.True(SpoolSink.HasUndelivered(dir));
            var second = new Stopping(int.MaxValue);
            spool = new SpoolSink(second, new SpoolOptions { Directory = dir, AckOn = SpoolAck.Delivered }); // the mode was changed: they still count
            await spool.StartAsync(CancellationToken.None);
            for (var i = 0; i < 1000 && spool.DeliveredSeq < spool.LastSeq; i++) await Task.Delay(1);
            await spool.DisposeAsync();
            Assert.Equal(["a"], second.Ids);
            Assert.False(SpoolSink.HasUndelivered(dir));
        });
    }

    /// <summary>When everything is delivered, a call must not wait for room that will never come.</summary>
    [HegelTest(TestCases = 60)]
    public void A_full_spool_that_is_delivered_takes_more(TestCase tc)
    {
        var dir = NewDirectory();
        var payload = tc.Draw(Generators.Integers().Min(1).Max(3000), "payload");
        var options = new SpoolOptions { Directory = dir, AckOn = SpoolAck.Delivered, SegmentBytes = tc.Draw(Generators.Integers().Min(1).Max(5000), "segment"), MaxBytes = tc.Draw(Generators.Integers().Min(1).Max(5000), "max"), CursorFlushInterval = TimeSpan.FromMilliseconds(1) };
        Blocking.Run(async () =>
        {
            var inner = new Stopping(int.MaxValue);
            await using var spool = new SpoolSink(inner, options);
            await spool.StartAsync(CancellationToken.None);
            for (var i = 0; i < 6; i++)
                await (await spool.SendAsync([new SinkMessage { Destination = "d", Key = "k", MessageId = "m" + i, Payload = new byte[payload] }], CancellationToken.None).AsTask().WaitAsync(TimeSpan.FromSeconds(5))).WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(6, inner.Count);
        });
    }

    // ------------------------------------------------------------------ checkpoints in a file

    private static Checkpoint DrawCheckpoint(TestCase tc, string db) => new(db, tc.Draw(Generators.SampledFrom(new[] { "", "ABC-2026" }), "incarnation"),
        new Lsn((uint)tc.Draw(Generators.Integers().Min(0), "r"), 1, 1), new Lsn((uint)tc.Draw(Generators.Integers().Min(0), "c"), 1, 2), default,
        DateTimeOffset.FromUnixTimeMilliseconds(tc.Draw(Generators.Integers().Min(0), "t")), tc.Draw(Generators.Booleans(), "held"));

    [HegelTest(TestCases = 400)]
    public void Saved_checkpoints_are_there_after_a_restart_or_damage_is_noticed(TestCase tc)
    {
        var path = Path.Combine(NewDirectory(), "cp.lmc");
        var saved = new List<Dictionary<string, Checkpoint>> { new(StringComparer.OrdinalIgnoreCase) };
        Blocking.Run(async () =>
        {
            for (var rounds = tc.Draw(Generators.Integers().Min(1).Max(4), "saves"); rounds > 0; rounds--)
            {
                using var store = new FileCheckpointStore(path);
                var state = new Dictionary<string, Checkpoint>(saved[^1], StringComparer.OrdinalIgnoreCase);
                var batch = tc.Draw(Generators.Lists(Generators.SampledFrom(new[] { "a", "A", "b\tc", "d\\n", "e\nf", "日本", "x|y" })).MinSize(1).MaxSize(3), "dbs").DistinctBy(d => d.ToUpperInvariant()).Select(d => DrawCheckpoint(tc, d)).ToList();
                foreach (var c in batch) { state.Remove(c.Database); state[c.Database] = c; }
                await store.SaveAsync(batch, CancellationToken.None);
                saved.Add(state);
            }
        });
        var damage = tc.Draw(Generators.SampledFrom(new[] { "none", "flip", "cut", "delete", "both" }), "damage");
        void Damage(string file)
        {
            var data = File.ReadAllBytes(file);
            if (tc.Draw(Generators.Booleans(), "cutIt")) data = data[..tc.Draw(Generators.Integers().Min(0).Max(data.Length - 1), "cutAt")];
            else data[tc.Draw(Generators.Integers().Min(0).Max(data.Length - 1), "at")] ^= (byte)(1 << tc.Draw(Generators.Integers().Min(0).Max(7), "bit"));
            File.WriteAllBytes(file, data);
        }
        if (damage is "flip" or "cut" or "both") Damage(path);
        if (damage == "delete") File.Delete(path);
        if (damage == "both" && File.Exists(path + ".bak")) Damage(path + ".bak");
        using var reader = new FileCheckpointStore(path);
        Dictionary<string, Checkpoint?> Load(IEnumerable<string> names) => names.ToDictionary(n => n, n => Blocking.Run(() => reader.LoadAsync(n, CancellationToken.None)));
        var names = saved.SelectMany(s => s.Keys).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        Dictionary<string, Checkpoint?> got;
        try { got = Load(names); }
        catch (InvalidDataException) { Assert.NotEqual("none", damage); return; } // loud: the operator decides
        bool Is(Dictionary<string, Checkpoint> state) => names.All(n => Equals(got[n], state.GetValueOrDefault(n)));
        Assert.True(damage == "none" ? Is(saved[^1]) : Is(saved[^1]) || Is(saved[^2]), $"{damage}: neither the last state nor the one before it");
    }

    // ------------------------------------------------------------------ event files

    [HegelTest(TestCases = 100)]
    public void Every_acknowledged_event_is_on_a_line_of_its_own_in_order(TestCase tc)
    {
        var dir = NewDirectory();
        var sent = new List<string>();
        Blocking.Run(async () =>
        {
            for (var runs = tc.Draw(Generators.Integers().Min(1).Max(3), "runs"); runs > 0; runs--)
            {
                await using var sink = new FileSink(new FileSinkOptions { Directory = dir, MaxFileBytes = tc.Draw(Generators.SampledFrom(new[] { 0L, 100L, 10_000L }), "maxFileBytes") });
                await sink.StartAsync(CancellationToken.None);
                for (var calls = tc.Draw(Generators.Integers().Min(1).Max(6), "calls"); calls > 0; calls--)
                {
                    var batch = Enumerable.Range(0, tc.Draw(Generators.Integers().Min(1).Max(3), "messages")).Select(i => new SinkMessage
                    {
                        Destination = "d", Key = tc.Draw(Generators.Text().MaxSize(5), "key"), MessageId = $"{sent.Count + i}", ContentType = tc.Draw(Generators.Booleans(), "json") ? "application/json" : null,
                        Payload = System.Text.Encoding.UTF8.GetBytes("{\"a\":1}"), Headers = [new("h", tc.Draw(Generators.Text().MaxSize(5), "header"))],
                    }).ToList();
                    await await sink.SendAsync(batch, CancellationToken.None);
                    sent.AddRange(batch.Select(m => m.MessageId));
                }
            }
        });
        var lines = Directory.GetFiles(dir, "*.jsonl").OrderBy(f => File.GetCreationTimeUtc(f)).ThenBy(f => f.Length).ThenBy(f => f, StringComparer.Ordinal).SelectMany(File.ReadAllLines).ToList();
        Assert.Equal(sent.Order(StringComparer.Ordinal), lines.Select(l => { using var doc = JsonDocument.Parse(l); return doc.RootElement.GetProperty("id").GetString()!; }).Order(StringComparer.Ordinal));
        foreach (var file in Directory.GetFiles(dir, "*.jsonl"))
        {
            var ids = File.ReadAllLines(file).Select(l => { using var doc = JsonDocument.Parse(l); return int.Parse(doc.RootElement.GetProperty("id").GetString()!, System.Globalization.CultureInfo.InvariantCulture); }).ToList();
            Assert.Equal(ids.Order(), ids);
        }
    }
}
