using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Hegel;
using Hegel.Xunit;
using LogMiner.Capture;
using LogMiner.Events;
using LogMiner.Rows;
using LogMiner.Schema;
using LogMiner.Sinks;
using Xunit;

namespace LogMiner.Core.Tests;

/// <summary>Message ids (the broker drops what it has seen: a collision is a lost event), values as JSON, headers and routing.</summary>
public class EventProperties
{
    /// <summary>Names as they come: any Unicode, with everything that separates something somewhere.</summary>
    private static readonly Generator<string> Names = Generators.OneOf(
        Generators.Lists(Generators.SampledFrom(new[] { "a", "A", "å", "ä", ":", "|", "/", ".", "#", "%", " ", "\n", "\r", "\t", "\0", "snap", "3A", "%3A", "0000002A", "日", "😀", "'", "\"", "\\", "*", ">" })).MinSize(1).MaxSize(6).Select(string.Concat),
        Generators.Text().MinSize(1).MaxSize(12));

    private static TableLayout Table(string database, string schema = "dbo", string table = "t") => new(5, database, 1, schema, table, 1, DataCompression.None,
    [
        new ColumnLayout { ColumnId = 1, PartitionColumnId = 1, Name = "id", Type = SqlType.Int, MaxLength = 4, MaxInRowLength = 4, LeafOffset = 4, LeafNullBit = 1, KeyOrdinal = 1 },
        new ColumnLayout { ColumnId = 2, PartitionColumnId = 2, Name = "v", Type = SqlType.NVarChar, MaxLength = -1, MaxInRowLength = 8000, LeafOffset = -1, LeafNullBit = 2, IsNullable = true },
        new ColumnLayout { ColumnId = 3, PartitionColumnId = 3, Name = "w", Type = SqlType.VarBinary, MaxLength = -1, MaxInRowLength = 8000, LeafOffset = -2, LeafNullBit = 3, IsNullable = true },
    ]);

    private sealed record Id(string Server, string Database, string Incarnation, Lsn Commit, int Sequence, string? Read);

    private static Id DrawId(TestCase tc, string label)
    {
        var lsns = new[] { new Lsn(1, 2, 3), new Lsn(1, 2, 4), new Lsn(1, 3, 3), new Lsn(0x2A, 0x6C8, 3) };
        return new Id(tc.Draw(Names, label + ".server"), tc.Draw(Names, label + ".db"),
            tc.Draw(Generators.SampledFrom(new[] { "", "06E43102AAAA", "06E43102AAAB", "06E43102AAAA-2026-11-2419:55:13.750", "06E43102AAAA-2026-11-2419:55:13.753" }), label + ".incarnation"),
            tc.Draw(Generators.SampledFrom(lsns), label + ".commit"), tc.Draw(Generators.Integers().Min(0).Max(12), label + ".seq"),
            tc.Draw(Generators.Booleans(), label + ".snapshot") ? tc.Draw(Generators.SampledFrom(new[] { "aaaa.1", "aaaa.11", "aaab.1" }), label + ".read") : null);
    }

    private static SinkMessage Message(Id x) => new EventEncoder(new EncoderOptions { ServerName = x.Server }).Encode(new ChangeEvent
    {
        Table = Table(x.Database), Op = x.Read is null ? ChangeOp.Create : ChangeOp.Read, After = new RowImage(Table(x.Database), [1, "x", null]),
        Lsn = x.Commit, CommitLsn = x.Commit, Sequence = x.Sequence, Incarnation = x.Incarnation, SnapshotId = x.Read is null ? null : "s", SnapshotRead = x.Read,
    });

    [HegelTest(TestCases = 4000)]
    public void Message_ids_are_equal_for_the_same_change_only(TestCase tc)
    {
        var a = DrawId(tc, "a");
        var other = DrawId(tc, "b");
        var b = tc.Draw(Generators.Integers().Min(0).Max(7), "differs in") switch // mostly in one thing: that one must be in the id
        {
            0 => a with { Server = other.Server }, 1 => a with { Database = other.Database }, 2 or 3 => a with { Incarnation = other.Incarnation },
            4 => a with { Commit = other.Commit }, 5 => a with { Sequence = other.Sequence }, 6 => a with { Read = other.Read }, _ => other,
        };
        var (x, y) = (Message(a), Message(b));
        // the rows of one reading have no commit: their number tells them apart
        var same = a with { Commit = default } == b with { Commit = default } && (a.Commit == b.Commit || a.Read is not null);
        Assert.Equal(same, x.MessageId == y.MessageId);
        Assert.All(x.MessageId, c => Assert.InRange(c, '!', '~'));
        Assert.Equal(x.MessageId, Message(a).MessageId);
    }

    /// <summary>A load that is started again, or run twice, reads rows that may have changed since: they are other events.</summary>
    [Fact]
    public void Two_readings_of_a_table_have_ids_of_their_own()
    {
        var ids = Enumerable.Range(0, 100).Select(_ => new Capture.TableSnapshot(Table("db"), "load", 5).ReadId).ToList();
        Assert.Equal(ids.Count, ids.Distinct().Count());
    }

    /// <summary>Reads a JSON string literal the way JSON defines it, also where it is not valid UTF-16 (System.Text.Json refuses to).</summary>
    private static string Unescape(string json)
    {
        Assert.True(json.Length >= 2 && json[0] == '"' && json[^1] == '"', json);
        var sb = new StringBuilder();
        for (var i = 1; i < json.Length - 1; i++)
        {
            Assert.True(json[i] >= ' ' && json[i] != '"', "unescaped control character or quote");
            if (json[i] != '\\') { sb.Append(json[i]); continue; }
            switch (json[++i])
            {
                case 'n': sb.Append('\n'); break; case 'r': sb.Append('\r'); break; case 't': sb.Append('\t'); break;
                case 'b': sb.Append('\b'); break; case 'f': sb.Append('\f'); break;
                case '"': case '\\': case '/': sb.Append(json[i]); break;
                case 'u': sb.Append((char)ushort.Parse(json.AsSpan(i + 1, 4), NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture)); i += 4; break;
                default: Assert.Fail($"unknown escape in {json}"); break;
            }
        }
        return sb.ToString();
    }

    private static string Json(object? value)
    {
        var buffer = new ArrayBufferWriter<byte>();
        using (var w = new Utf8JsonWriter(buffer, new JsonWriterOptions { SkipValidation = true })) ChangeEventJson.WriteValue(w, value);
        return new UTF8Encoding(false, throwOnInvalidBytes: true).GetString(buffer.WrittenSpan);
    }

    /// <summary>nvarchar holds any sequence of UTF-16 units, also half a surrogate pair.</summary>
    [HegelTest(TestCases = 4000)]
    public void Any_nvarchar_value_comes_out_exactly(TestCase tc)
    {
        var units = tc.Draw(Generators.Lists(Generators.OneOf(Generators.Integers().Min(0).Max(0xFFFF), Generators.Integers().Min(0xD7FE).Max(0xE001), Generators.Integers().Min(0).Max(0x7F))).MaxSize(24), "units");
        var raw = new byte[units.Count * 2];
        for (var i = 0; i < units.Count; i++) BitConverter.TryWriteBytes(raw.AsSpan(2 * i), (ushort)units[i]);
        var column = new ColumnLayout { ColumnId = 1, PartitionColumnId = 1, Name = "c", Type = SqlType.NVarChar, MaxLength = 4000, MaxInRowLength = 4000, LeafOffset = -1, LeafNullBit = 1 };
        var value = Assert.IsType<string>(ValueDecoder.Decode(raw, column));
        Assert.Equal(units, value.Select(c => (int)c));
        Assert.Equal(units, Unescape(Json(value)).Select(c => (int)c));
    }

    private static readonly int[] CodePages = [437, 850, 874, 932, 936, 949, 950, 1250, 1251, 1252, 1253, 1254, 1255, 1256, 1257, 1258, 65001];

    /// <summary>char and varchar hold any bytes. What is published as text encodes to the same bytes; what does not is marked, not changed.</summary>
    [HegelTest(TestCases = 4000)]
    public void Any_varchar_value_comes_out_exactly_or_marked(TestCase tc)
    {
        var codePage = tc.Draw(Generators.SampledFrom(CodePages), "codePage");
        var raw = tc.Draw(Generators.Binary().MaxSize(16), "raw");
        var column = new ColumnLayout { ColumnId = 1, PartitionColumnId = 1, Name = "c", Type = SqlType.VarChar, MaxLength = 8000, MaxInRowLength = 8000, CodePage = codePage, LeafOffset = -1, LeafNullBit = 1 };
        switch (ValueDecoder.Decode(raw, column))
        {
            case string s:
                Assert.Equal(raw, ValueDecoder.EncodingFor(codePage).GetBytes(s));
                Assert.Equal(s, Unescape(Json(s)));
                break;
            case RawText t:
                Assert.Equal(raw, t.Bytes);
                using (var doc = JsonDocument.Parse(Json(t))) Assert.Equal(raw, doc.RootElement.GetProperty("undecodable").GetBytesFromBase64());
                break;
            default: Assert.Fail("neither text nor bytes"); break;
        }
    }

    /// <summary>Text that is what it says in every code page is never marked.</summary>
    [HegelTest(TestCases = 1000)]
    public void Text_of_a_code_page_is_text(TestCase tc)
    {
        var codePage = tc.Draw(Generators.SampledFrom(new[] { 1250, 1251, 1252, 1253, 1254, 932, 936, 65001 }), "codePage");
        var sample = codePage switch { 1251 => "Привет", 932 => "日本語カナ", 936 => "中文", 1253 => "Ελλάδα", 1254 => "İıŞğ", 1250 => "Žluťoučký", _ => "Åäö€" };
        var text = string.Concat(tc.Draw(Generators.Lists(Generators.SampledFrom(sample.Select(c => c.ToString()).Append("x").Append(" ").ToArray())).MaxSize(20), "text"));
        Assert.Equal(text, ValueDecoder.DecodeText(ValueDecoder.EncodingFor(codePage).GetBytes(text), codePage));
    }

    [HegelTest(TestCases = 2000)]
    public void Numbers_come_out_exactly(TestCase tc)
    {
        var d = tc.Draw(Generators.Doubles(), "double");
        if (double.IsFinite(d)) Assert.Equal(BitConverter.DoubleToInt64Bits(d), BitConverter.DoubleToInt64Bits(double.Parse(Json(d), CultureInfo.InvariantCulture)));
        else Assert.Equal(d.ToString(CultureInfo.InvariantCulture), Unescape(Json(d)));
        var f = (float)d;
        if (float.IsFinite(f)) Assert.Equal(BitConverter.SingleToInt32Bits(f), BitConverter.SingleToInt32Bits(float.Parse(Json(f), CultureInfo.InvariantCulture)));
        var l = tc.Draw(Generators.Longs(), "long");
        Assert.Equal(l.ToString(CultureInfo.InvariantCulture), Json(l));
        var m = new decimal(l) / 10000m;
        Assert.Equal(m, decimal.Parse(Unescape(Json(m)), CultureInfo.InvariantCulture));
        var t = new DateTime((l & long.MaxValue) % DateTime.MaxValue.Ticks);
        Assert.Equal(t, DateTime.ParseExact(Unescape(Json(t)), "yyyy-MM-ddTHH:mm:ss.fffffff", CultureInfo.InvariantCulture));
    }

    [HegelTest(TestCases = 2000)]
    public void Header_values_have_no_line_breaks_and_stay_different(TestCase tc)
    {
        var (a, b) = (tc.Draw(Names, "a"), tc.Draw(Names, "b"));
        Assert.DoesNotContain(EventEncoder.HeaderValue(a), c => c < ' ' || c == '\x7f');
        Assert.Equal(EventEncoder.HeaderValue(a).Trim(' '), EventEncoder.HeaderValue(a));
        Assert.Equal(a == b, EventEncoder.HeaderValue(a) == EventEncoder.HeaderValue(b));
    }

    [HegelTest(TestCases = 2000)]
    public void Destinations_have_the_parts_of_the_template(TestCase tc)
    {
        var parts = tc.Draw(Generators.Lists(Generators.SampledFrom(new[] { "cdc", "{server}", "{db}", "{schema}", "{table}", "{op}", "{DB}", "", "x y", "*", ">", "a-{db}_b" })).MinSize(1).MaxSize(6), "parts");
        var template = string.Join('.', parts);
        var problem = EventEncoder.TemplateProblem(template);
        Assert.Equal(parts.Any(p => p is "{DB}" or "" or "x y" or "*" or ">"), problem is not null);
        if (problem is not null) return;
        var db = tc.Draw(Names, "db");
        var schema = tc.Draw(Names, "schema");
        var message = new EventEncoder(new EncoderOptions { ServerName = tc.Draw(Names, "server"), DestinationTemplate = template }).Encode(new ChangeEvent
        {
            Table = Table(db, schema, tc.Draw(Names, "table")), Op = ChangeOp.Create, Lsn = new Lsn(1, 1, 1), CommitLsn = new Lsn(1, 1, 1), Sequence = 0,
        });
        Assert.Equal(parts.Count, message.Destination.Split('.').Length);
        Assert.All(message.Destination, c => Assert.True(char.IsAsciiLetterOrDigit(c) || c is '_' or '-' or '.'));
    }

    /// <summary>An event that is too large for the destination must not stop its database: its largest values are marked, the key never is,
    /// and everything else stays what it is.</summary>
    [HegelTest(TestCases = 500)]
    public void An_event_that_is_too_large_comes_without_its_largest_values(TestCase tc)
    {
        var table = Table("db");
        RowImage Image(string label) => new(table, [7, new string('x', tc.Draw(Generators.Integers().Min(0).Max(20_000), label + ".v")), new byte[tc.Draw(Generators.Integers().Min(0).Max(20_000), label + ".w")]]);
        var e = new ChangeEvent { Table = table, Op = ChangeOp.Update, Before = Image("before"), After = Image("after"), Lsn = new Lsn(1, 1, 1), CommitLsn = new Lsn(1, 1, 1), Sequence = 0 };
        var encoder = new EventEncoder(new EncoderOptions { ServerName = "s" });
        var limit = tc.Draw(Generators.Integers().Min(1200).Max(100_000), "limit");
        var m = encoder.Encode(e, limit);
        Assert.True(m.Payload.Length <= limit);
        using var doc = JsonDocument.Parse(m.Payload);
        var marked = 0;
        foreach (var (name, image) in new[] { ("before", e.Before!), ("after", e.After!) })
        {
            var got = doc.RootElement.GetProperty(name);
            Assert.Equal(7, got.GetProperty("id").GetInt32());
            if (got.GetProperty("v").ValueKind == JsonValueKind.Object) marked++; else Assert.Equal(image.Values[1], got.GetProperty("v").GetString());
            if (got.GetProperty("w").ValueKind == JsonValueKind.Object) marked++; else Assert.Equal((byte[])image.Values[2]!, got.GetProperty("w").GetBytesFromBase64());
        }
        Assert.Equal(encoder.Encode(e).Payload.Length > limit, marked > 0); // nothing is marked without need
        Assert.Equal(encoder.Encode(e).MessageId, m.MessageId);
    }

    /// <summary>Historical loads drop a row that was changed meanwhile, found by its key: two keys are the same exactly when their values are.</summary>
    [HegelTest(TestCases = 4000)]
    public void Keys_are_the_same_only_when_their_values_are(TestCase tc)
    {
        var kinds = tc.Draw(Generators.Lists(Generators.Integers().Min(0).Max(9)).MinSize(1).MaxSize(3), "kinds");
        var layout = new TableLayout(5, "db", 1, "dbo", "t", 1, DataCompression.None, kinds.Select((_, i) => new ColumnLayout
        { ColumnId = i + 1, PartitionColumnId = i + 1, Name = "k" + i, Type = SqlType.Int, MaxLength = 4, MaxInRowLength = 4, LeafOffset = 4, LeafNullBit = (short)(i + 1), KeyOrdinal = i + 1 }).ToArray());
        object? Draw(int kind, string label) => kind switch
        {
            0 => tc.Draw(Generators.Integers().Min(0).Max(20), label),
            1 => string.Concat(tc.Draw(Generators.Lists(Generators.SampledFrom(new[] { "a", "\u001f", "\0", "1:", "n", "s", "\ud83d", "\ud83e", "é" })).MaxSize(4), label)),
            2 => new DateTime(2026, 1, 1).AddTicks(tc.Draw(Generators.Integers().Min(0).Max(3), label)),
            3 => new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero).AddMilliseconds(tc.Draw(Generators.Integers().Min(0).Max(3), label)),
            4 => TimeSpan.FromTicks(tc.Draw(Generators.Integers().Min(0).Max(3), label)),
            5 => tc.Draw(Generators.Binary().MaxSize(3), label),
            6 => new decimal(tc.Draw(Generators.Integers().Min(0).Max(5), label)) / 10m,
            7 => tc.Draw(Generators.Booleans(), label) ? null : tc.Draw(Generators.SampledFrom(new[] { "", "n", "\0" }), label + "s"),
            8 => new RawText(tc.Draw(Generators.Binary().MaxSize(2), label)),
            _ => DateOnly.FromDayNumber(tc.Draw(Generators.Integers().Min(0).Max(3), label)),
        };
        var a = kinds.Select((k, i) => Draw(k, $"a{i}")).ToArray();
        var b = tc.Draw(Generators.Booleans(), "equal") ? a : kinds.Select((k, i) => Draw(k, $"b{i}")).ToArray();
        var same = a.Zip(b).All(p => p.First is byte[] x ? p.Second is byte[] y && x.AsSpan().SequenceEqual(y) : p.First is string s ? p.Second is string t && string.Equals(s, t, StringComparison.Ordinal) : Equals(p.First, p.Second));
        Assert.Equal(same, TableSnapshot.KeyOf(new RowImage(layout, a)) == TableSnapshot.KeyOf(new RowImage(layout, b)));
        a[^1] = Unavailable.Value;
        Assert.Null(TableSnapshot.KeyOf(new RowImage(layout, a)));
    }
}
