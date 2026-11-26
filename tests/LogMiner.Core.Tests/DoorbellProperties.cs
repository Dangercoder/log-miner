using System.Buffers.Binary;
using System.Text;
using Hegel;
using Hegel.Xunit;
using LogMiner.Capture;
using LogMiner.Log;
using Xunit;

namespace LogMiner.Core.Tests;

/// <summary>The decoder of the live event stream: fixtures recorded from SQL Server 2022 CU28 (experiments/xe-fixtures), and generated packets.</summary>
public class LogFlushDecoderProperties
{
    private static readonly (string, string)[] Columns =
    [
        ("database_id", "uint32"), ("file_id", "uint32"), ("log_block_id", "uint64"), ("duration", "uint64"), ("write_size", "uint32"),
        ("rows", "uint32"), ("pending_writes", "uint32"), ("pending_bytes", "uint32"), ("start_timestamp", "uint64"), ("pending_buffers", "uint32"),
    ];

    private static readonly Generator<LogFlush> Flushes = Generators.Composite(tc => new LogFlush(
        tc.Draw(Generators.Integers().Min(1).Max(32767), "db"),
        new Lsn((uint)tc.Draw(Generators.Longs().Min(0).Max(uint.MaxValue), "vlf"), (uint)tc.Draw(Generators.Longs().Min(0).Max(uint.MaxValue), "block"), 0),
        (uint)tc.Draw(Generators.Longs().Min(0).Max(uint.MaxValue), "size")));

    private static byte[] Metadata(uint id, int padding)
    {
        var name = Encoding.Unicode.GetBytes(LogFlushDecoder.EventName + "\0");
        var b = new byte[padding + 4 + name.Length + 40];
        BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(padding), id);
        name.CopyTo(b, padding + 4);
        return b;
    }

    /// <summary>An event packet, as the engine frames it; only the fields the decoder must not depend on are arbitrary.</summary>
    private static byte[] Packet(uint id, IReadOnlyList<LogFlush> flushes, int firstEvent = 77, byte filler = 0xCC)
    {
        const int eventSize = 24 + 52;
        var b = new byte[firstEvent + flushes.Count * eventSize];
        b.AsSpan().Fill(filler);
        BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(12), 10);
        BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(16), (uint)b.Length);
        BinaryPrimitives.WriteUInt16LittleEndian(b.AsSpan(22), (ushort)firstEvent);
        BinaryPrimitives.WriteUInt32LittleEndian(b.AsSpan(24), (uint)(flushes.Count * eventSize));
        for (var i = 0; i < flushes.Count; i++)
        {
            var e = b.AsSpan(firstEvent + i * eventSize, eventSize);
            BinaryPrimitives.WriteUInt32LittleEndian(e, id);
            BinaryPrimitives.WriteUInt32LittleEndian(e[4..], 10);
            BinaryPrimitives.WriteUInt32LittleEndian(e[20..], 52);
            BinaryPrimitives.WriteUInt32LittleEndian(e[24..], (uint)flushes[i].DatabaseId);
            BinaryPrimitives.WriteUInt64LittleEndian(e[32..], ((ulong)flushes[i].Block.Vlf << 32) | flushes[i].Block.Block);
            BinaryPrimitives.WriteUInt32LittleEndian(e[48..], flushes[i].WriteSize);
        }
        return b;
    }

    private static LogFlushDecoder Ready(uint id)
    {
        var d = new LogFlushDecoder(Columns);
        d.Decode(LogFlushDecoder.MetadataPacket, Metadata(id, 32), []);
        Assert.True(d.Ready);
        return d;
    }

    private static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "xe-fixtures", name);

    [HegelTest]
    public void Every_flush_of_a_packet_comes_out_in_order(TestCase tc)
    {
        var flushes = tc.Draw(Generators.Lists(Flushes).MaxSize(40), "flushes");
        var id = (uint)tc.Draw(Generators.Longs().Min(1).Max(uint.MaxValue), "id");
        var first = tc.Draw(Generators.Integers().Min(32).Max(200), "first event");
        var into = new List<LogFlush>();
        Ready(id).Decode(LogFlushDecoder.EventPacket, Packet(id, flushes, first, (byte)tc.Draw(Generators.Integers().Min(0).Max(255), "filler")), into);
        Assert.Equal(flushes, into);
    }

    [HegelTest]
    public void A_damaged_packet_is_refused_as_a_whole_or_understood_and_nothing_else_is_thrown(TestCase tc)
    {
        const uint id = 0x0BE808;
        var flushes = tc.Draw(Generators.Lists(Flushes).MinSize(1).MaxSize(6), "flushes");
        var packet = Packet(id, flushes);
        var known = tc.Draw(Generators.Lists(Flushes).MaxSize(2), "earlier");
        var into = new List<LogFlush>(known);
        for (var i = tc.Draw(Generators.Integers().Min(1).Max(4), "damages"); i > 0; i--)
        {
            switch (tc.Draw(Generators.Integers().Min(0).Max(2), "kind"))
            {
                case 0: packet = packet[..tc.Draw(Generators.Integers().Min(0).Max(packet.Length), "cut")]; break;
                case 1: packet = [.. packet, .. new byte[tc.Draw(Generators.Integers().Min(1).Max(100), "extra")]]; break;
                default:
                    if (packet.Length > 0) packet[tc.Draw(Generators.Integers().Min(0).Max(packet.Length - 1), "at")] = (byte)tc.Draw(Generators.Integers().Min(0).Max(255), "value");
                    break;
            }
        }
        try
        {
            Ready(id).Decode(LogFlushDecoder.EventPacket, packet, into);
            Assert.InRange(into.Count - known.Count, 0, flushes.Count);
        }
        catch (EventStreamFormatException) { Assert.Equal(known, into); }
        Assert.All(into, f => Assert.InRange(f.DatabaseId, 1, 32767));
    }

    [HegelTest]
    public void Arbitrary_bytes_never_crash_it(TestCase tc)
    {
        var d = tc.Draw(Generators.Booleans(), "ready") ? Ready(7) : new LogFlushDecoder(Columns);
        var bytes = tc.Draw(Generators.Lists(Generators.Integers().Min(0).Max(255)).MaxSize(300), "bytes").Select(b => (byte)b).ToArray();
        var into = new List<LogFlush>();
        try { d.Decode(tc.Draw(Generators.Integers().Min(-1).Max(4), "type"), bytes, into); }
        catch (EventStreamFormatException) { Assert.Empty(into); }
    }

    [HegelTest]
    public void Fields_are_found_by_the_column_list_not_by_position(TestCase tc)
    {
        // the same event with another column in front, as a cumulative update might add
        var type = tc.Draw(Generators.SampledFrom(new[] { ("uint8", 1), ("boolean", 1), ("uint16", 2), ("uint32", 4), ("uint64", 8) }), "type");
        var flush = tc.Draw(Flushes, "flush");
        var d = new LogFlushDecoder([("new_column", type.Item1), .. Columns]);
        d.Decode(LogFlushDecoder.MetadataPacket, Metadata(5, 10), []);
        var plain = Packet(5, [flush]);
        byte[] packet = [.. plain[..(77 + 24)], .. new byte[type.Item2], .. plain[(77 + 24)..]];
        BinaryPrimitives.WriteUInt32LittleEndian(packet.AsSpan(16), (uint)packet.Length);
        BinaryPrimitives.WriteUInt32LittleEndian(packet.AsSpan(24), (uint)(packet.Length - 77));
        BinaryPrimitives.WriteUInt32LittleEndian(packet.AsSpan(77 + 20), (uint)(52 + type.Item2));
        var into = new List<LogFlush>();
        d.Decode(LogFlushDecoder.EventPacket, packet, into);
        Assert.Equal([flush], into);
        Assert.Throws<EventStreamFormatException>(() => Ready(5).Decode(LogFlushDecoder.EventPacket, packet, [])); // without knowing the column: refused
    }

    [Fact]
    public void What_it_does_not_understand_is_refused()
    {
        Assert.Throws<EventStreamFormatException>(() => new LogFlushDecoder(Columns.Where(c => c.Item1 != "write_size")));
        Assert.Throws<EventStreamFormatException>(() => new LogFlushDecoder([.. Columns, ("text", "unicode_string")]));
        Assert.Throws<EventStreamFormatException>(() => new LogFlushDecoder(Columns.Select(c => c.Item1 == "database_id" ? (c.Item1, "uint64") : c)));
        var d = new LogFlushDecoder(Columns);
        Assert.Throws<EventStreamFormatException>(() => d.Decode(LogFlushDecoder.EventPacket, Packet(1, []), [])); // no metadata yet
        Assert.Throws<EventStreamFormatException>(() => d.Decode(LogFlushDecoder.MetadataPacket, new byte[500], []));
        Assert.Throws<EventStreamFormatException>(() => d.Decode(LogFlushDecoder.MetadataPacket, [.. Metadata(1, 8), .. Metadata(2, 8)], []));
        Assert.Throws<EventStreamFormatException>(() => d.Decode(LogFlushDecoder.MetadataPacket, Metadata(0, 8), []));
        Assert.Throws<EventStreamFormatException>(() => d.Decode(LogFlushDecoder.HeaderPacket, new byte[24], []));
        Assert.Throws<EventStreamFormatException>(() => d.Decode(3, new byte[24], []));
        Assert.False(d.Ready);
        Assert.Throws<EventStreamFormatException>(() => Ready(1).Decode(LogFlushDecoder.EventPacket, Packet(2, [new LogFlush(5, new Lsn(1, 8, 0), 512)]), []));
        Assert.Throws<EventStreamFormatException>(() => Ready(1).Decode(LogFlushDecoder.EventPacket, Packet(1, [new LogFlush(32768, new Lsn(1, 8, 0), 512)]), []));
    }

    /// <summary>Every checked field of an event's header, in any event of a packet: one wrong value and nothing of the packet is taken.</summary>
    [HegelTest]
    public void One_wrong_header_field_and_nothing_of_the_packet_is_taken(TestCase tc)
    {
        var flushes = tc.Draw(Generators.Lists(Flushes).MinSize(1).MaxSize(5), "flushes");
        var packet = Packet(9, flushes);
        var e = 77 + tc.Draw(Generators.Integers().Min(0).Max(flushes.Count - 1), "event") * 76;
        var (at, right) = tc.Draw(Generators.SampledFrom(new[] { (e, 9u), (e + 4, 10u), (e + 20, 52u) }), "field");
        var wrong = (uint)tc.Draw(Generators.Longs().Min(0).Max(uint.MaxValue), "value");
        tc.Assume(wrong != right);
        BinaryPrimitives.WriteUInt32LittleEndian(packet.AsSpan(at), wrong);
        var earlier = new LogFlush(3, new Lsn(1, 2, 0), 512);
        var into = new List<LogFlush> { earlier };
        Assert.Throws<EventStreamFormatException>(() => Ready(9).Decode(LogFlushDecoder.EventPacket, packet, into));
        Assert.Equal([earlier], into);
    }

    /// <summary>Packets of SQL Server 2022 CU28 while 12 databases were written concurrently, and the growth of num_of_bytes_written over the
    /// same time: every flush is decoded, and their sizes add up to what was written except for the log file's own headers (12 or 24 KB).</summary>
    [Fact]
    public void Recorded_packets_account_for_every_byte_written()
    {
        var d = new LogFlushDecoder(Columns);
        var flushes = new List<LogFlush>();
        foreach (var file in Directory.GetFiles(Fixture("stream"), "*.bin").Order(StringComparer.Ordinal))
            d.Decode(file[^5] - '0', File.ReadAllBytes(file), flushes);
        Assert.True(flushes.Count > 100, $"{flushes.Count} flushes");
        var pushed = flushes.GroupBy(f => f.DatabaseId).ToDictionary(g => g.Key, g => g.Sum(f => (long)f.WriteSize));
        foreach (var line in File.ReadLines(Fixture("stream/bytes-written.txt")))
        {
            var parts = line.Split(' ');
            var (db, written) = (int.Parse(parts[0], System.Globalization.CultureInfo.InvariantCulture), long.Parse(parts[1], System.Globalization.CultureInfo.InvariantCulture));
            Assert.InRange(written - pushed[db], 0, 24576);
            Assert.Equal(0, (written - pushed[db]) % 4096);
        }
        Assert.Equal(flushes.Count, flushes.Select(f => (f.DatabaseId, f.Block)).Distinct().Count()); // every block once - but not in order: writes complete out of order
    }
}

/// <summary>
/// The audit of the push against polled bytes, as a model: writes happen, their events arrive later (or earlier than the poll sees the
/// bytes), get lost, or the engine writes to the file without any event.
/// </summary>
public class LogWriteAuditProperties
{
    [HegelTest]
    public void A_write_the_push_lost_is_found_by_the_next_poll_and_pushed_writes_are_not_read_again(TestCase tc)
    {
        var audit = new LogWriteAudit();
        const int dbs = 3;
        var written = new long[dbs + 1];           // bytes the engine has written
        var counted = new long[dbs + 1];           // of which the DMV shows this many
        var inFlight = new List<(LogFlush Flush, bool Old)>(); // events not delivered yet (Old: the last poll was before their delivery)
        var late = new int[dbs + 1];               // consecutive polls at which the database had events on their way
        var lostSincePoll = new HashSet<int>();    // databases with a lost event (or an unannounced write) whose bytes the next poll shows
        var quiet = new int[dbs + 1];              // consecutive polls in which nothing odd was going on for the database
        List<(int, long)> Poll() => Enumerable.Range(1, dbs).Select(db => (db, counted[db])).ToList();
        audit.Polled(Poll(), pushing: true);
        for (var step = tc.Draw(Generators.Integers().Min(1).Max(120), "steps"); step > 0; step--)
        {
            var db = tc.Draw(Generators.Integers().Min(1).Max(dbs), "db");
            var size = (uint)(512 * tc.Draw(Generators.Integers().Min(1).Max(120), "sectors"));
            switch (tc.Draw(Generators.Integers().Min(0).Max(7), "what"))
            {
                case 0 or 1: // an ordinary write: bytes and event, the event delivered at some point
                    written[db] += size;
                    counted[db] = written[db];
                    inFlight.Add((new LogFlush(db, default, size), false));
                    break;
                case 2: // the event overtakes the counter
                    written[db] += size;
                    audit.Pushed([new LogFlush(db, default, size)]);
                    quiet[db] = 0;
                    break;
                case 3: // the event is lost
                    written[db] += size;
                    counted[db] = written[db];
                    lostSincePoll.Add(db);
                    break;
                case 4 or 5: // events arrive
                    audit.Pushed(inFlight.Select(f => f.Flush).ToList());
                    inFlight.Clear();
                    break;
                default:
                    // an event may be on its way while one poll is taken, not while two are (they are a second apart)
                    audit.Pushed(inFlight.Where(f => f.Old).Select(f => f.Flush).ToList());
                    inFlight = inFlight.Where(f => !f.Old).Select(f => (f.Flush, true)).ToList();
                    for (var i = 1; i <= dbs; i++)
                    {
                        // ... and not at every single one of many polls
                        late[i] = inFlight.Any(f => f.Flush.DatabaseId == i) ? late[i] + 1 : 0;
                        if (late[i] < LogWriteAudit.Window) continue;
                        audit.Pushed(inFlight.Where(f => f.Flush.DatabaseId == i).Select(f => f.Flush).ToList());
                        inFlight.RemoveAll(f => f.Flush.DatabaseId == i);
                        late[i] = 0;
                    }
                    for (var i = 1; i <= dbs; i++) counted[i] = written[i];
                    var read = audit.Polled(Poll(), pushing: true);
                    Assert.All(lostSincePoll, l => Assert.Contains(l, read));
                    for (var i = 1; i <= dbs; i++)
                    {
                        var odd = lostSincePoll.Contains(i) || inFlight.Any(f => f.Flush.DatabaseId == i);
                        // once the last oddity is out of the window, whatever the push reported is not read again
                        if (!odd && quiet[i] >= LogWriteAudit.Window) Assert.DoesNotContain(i, read);
                        quiet[i] = odd ? 0 : quiet[i] + 1;
                    }
                    lostSincePoll.Clear();
                    break;
            }
        }
    }

    [HegelTest]
    public void Without_the_push_every_database_that_wrote_is_read_exactly_then(TestCase tc)
    {
        var audit = new LogWriteAudit();
        var bytes = new SortedDictionary<int, long>();
        var seen = new Dictionary<int, long>(); // what the audit last saw
        for (var step = tc.Draw(Generators.Integers().Min(1).Max(30), "steps"); step > 0; step--)
        {
            foreach (var db in tc.Draw(Generators.Lists(Generators.Integers().Min(1).Max(6)).MaxSize(4), "writers"))
                bytes[db] = bytes.GetValueOrDefault(db) + 512;
            if (tc.Draw(Generators.Booleans(), "pushed as well")) audit.Pushed([new LogFlush(tc.Draw(Generators.Integers().Min(1).Max(6), "db"), default, 512)]);
            // a poll of some (the recently active ones) must not disturb the rest
            var complete = tc.Draw(Generators.Booleans(), "complete");
            var poll = bytes.Where(b => complete || (seen.ContainsKey(b.Key) && tc.Draw(Generators.Booleans(), "polled"))).Select(b => (b.Key, b.Value)).ToList();
            var expected = poll.Where(x => seen.TryGetValue(x.Key, out var before) && before != x.Value).Select(x => x.Key).ToArray();
            Assert.Equal(expected, audit.Polled(poll, pushing: false, complete).Order());
            foreach (var (db, b) in poll) seen[db] = b;
        }
    }

    /// <summary>Writes the push missed while it was detached are found by the first poll after it is back.</summary>
    [HegelTest]
    public void What_was_written_during_an_outage_is_found_when_the_push_is_back(TestCase tc)
    {
        var audit = new LogWriteAudit();
        long a = 0, b = 0;
        void Both(uint size, bool pushed)
        {
            a += size;
            b += size;
            if (pushed) audit.Pushed([new LogFlush(1, default, size), new LogFlush(2, default, size)]);
        }
        audit.Polled([(1, a), (2, b)], true);
        for (var i = tc.Draw(Generators.Integers().Min(0).Max(5), "before"); i > 0; i--) { Both(1024, true); audit.Polled([(1, a), (2, b)], true); }
        for (var i = tc.Draw(Generators.Integers().Min(0).Max(3), "outage polls"); i > 0; i--) { Both(512, false); Assert.Equal([1, 2], audit.Polled([(1, a), (2, b)], false)); }
        a += 4096; // only database 1, after the last poll of the outage
        Both(2048, true);
        Assert.Contains(1, audit.Polled([(1, a), (2, b)], true));
    }

    [HegelTest]
    public void A_silent_push_is_noticed_and_a_working_one_is_not_suspected(TestCase tc)
    {
        var audit = new LogWriteAudit();
        long bytes = 0;
        audit.Polled([(1, 0)], true);
        for (var i = tc.Draw(Generators.Integers().Min(0).Max(20), "working polls"); i > 0; i--)
        {
            if (tc.Draw(Generators.Booleans(), "write"))
            {
                bytes += 512;
                audit.Pushed([new LogFlush(1, default, 512)]);
            }
            audit.Polled([(1, bytes)], true);
            Assert.False(audit.Stalled);
        }
        for (var silent = 1; silent <= LogWriteAudit.StalledAfter; silent++)
        {
            for (var idle = tc.Draw(Generators.Integers().Min(0).Max(4), "idle polls"); idle > 0; idle--) audit.Polled([(1, bytes)], true);
            Assert.False(audit.Stalled); // nothing written, nothing to push
            bytes += 512;
            Assert.Equal([1], audit.Polled([(1, bytes)], true)); // unpushed writes are found meanwhile
        }
        Assert.True(audit.Stalled);
        audit.Polled([(1, bytes + 512)], pushing: false); // the push was given up: everything is polled, and the next stream starts afresh
        Assert.False(audit.Stalled);
    }

    [Fact]
    public void A_replaced_log_file_is_read_and_a_new_database_is_not_taken_for_a_missed_write()
    {
        var audit = new LogWriteAudit();
        Assert.Empty(audit.Polled([(1, 5000)], true));
        Assert.Equal([1], audit.Polled([(1, 100), (2, 700)], true)); // 1 was re-created; 2 is new (the engine reads new databases anyway)
        Assert.Equal([2], audit.Polled([(1, 100), (2, 1212)], true));
        Assert.Empty(audit.Polled([(2, 1212)], true));
        Assert.Empty(audit.Polled([(1, 100), (2, 1212)], true)); // 1 is back: new again
    }
}
