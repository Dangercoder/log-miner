using Hegel;
using Hegel.Xunit;
using LogMiner.Log;
using LogMiner.Schema;
using Xunit;

namespace LogMiner.Core.Tests;

/// <summary>Table names as of the change, from the changes of sys.sysschobjs in the log (CatalogHistory.Renames, DatabaseSchema.Renamed).</summary>
public class CatalogRenameProperties
{
    private static ColumnLayout Fixed(int id, string name, SqlType type, short size, short offset) =>
        new() { ColumnId = id, PartitionColumnId = id, Name = name, Type = type, MaxLength = size, MaxInRowLength = size, LeafOffset = offset, LeafNullBit = (short)id };

    // id int (4), nsid int (8), name nvarchar: the name is the only variable-length column, as in sys.sysschobjs
    private static readonly TableLayout Objects = new(5, "db", 34, "sys", "sysschobjs", 1, DataCompression.None,
    [
        Fixed(1, "id", SqlType.Int, 4, 4), Fixed(2, "nsid", SqlType.Int, 4, 8),
        new ColumnLayout { ColumnId = 3, PartitionColumnId = 3, Name = "name", Type = SqlType.NVarChar, MaxLength = 256, MaxInRowLength = 256, LeafOffset = -1, LeafNullBit = 3 },
    ]);
    private static readonly TableLayout Rowsets = new(5, "db", 5, "sys", "sysrowsets", 2, DataCompression.None,
        [Fixed(1, "rowsetid", SqlType.BigInt, 8, 4), Fixed(2, "idmajor", SqlType.Int, 4, 12), Fixed(3, "idminor", SqlType.Int, 4, 16)]);
    private static readonly Dictionary<CatalogTable, TableLayout> Formats = new() { [CatalogTable.Objects] = Objects, [CatalogTable.Rowsets] = Rowsets };

    private const int NameAt = 12 + 2 + 1 + 2; // fixed part, column count, null bitmap, number of variable columns
    private const int Table = 7;

    private static byte[] Key(int objectId) => [0x16, .. BitConverter.GetBytes(objectId)];

    private static CatalogChange Modify(uint n, int at, byte[] before, byte[] after) => new()
    {
        Lsn = new Lsn(1, n, 1), TransactionId = n, Operation = LogOperation.ModifyRow, Table = CatalogTable.Objects, Key = Key(Table), Offset = (short)at, Row0 = before, Row1 = after,
    };

    /// <summary>The row of sysschobjs as it is on the page.</summary>
    private static byte[] Row(int id, int nsid, string name)
    {
        var text = System.Text.Encoding.Unicode.GetBytes(name);
        return [0x30, 0x00, 12, 0, .. BitConverter.GetBytes(id), .. BitConverter.GetBytes(nsid), 3, 0, 0, 1, 0, .. BitConverter.GetBytes((ushort)(NameAt + 2 + text.Length)), .. text];
    }

    private static CatalogSnapshot Now(Dictionary<int, TableIdentity> objects, Dictionary<int, string> schemas) => new()
    {
        DatabaseId = 5, Database = "db", Rowsets = new Dictionary<long, CurrentRowset>(), Objects = objects, SchemaNames = schemas, CodePage = _ => 1252,
    };

    private static Dictionary<ulong, TransactionOutcome> Committed(params CatalogChange[] changes) =>
        changes.Select(c => c.TransactionId).Distinct().ToDictionary(x => x, x => new TransactionOutcome(false, false, new Lsn(1, (uint)x, 9)));

    [Fact]
    public void A_rename_is_found()
    {
        var rename = Modify(10, NameAt + 2, [(byte)'A'], [(byte)'B']);
        var history = CatalogHistory.Build(Now(new() { [Table] = new TableIdentity(Table, "dbo", "B", new Dictionary<int, string>(), new Dictionary<int, int>()) }, new() { [1] = "dbo" }),
            [rename], Committed(rename), Formats, default);
        Assert.Equal(("dbo", "A"), history.NameAt(Table, new Lsn(1, 5, 1)));
        Assert.Null(history.NameAt(Table, new Lsn(1, 15, 1)));
    }

    /// <summary>orders -> orders_old, then DROP TABLE orders_old, all of it before the miner reads that log (it was down, or behind): the changes made
    /// while the table was called A must come out as A.</summary>
    [Fact]
    public void A_table_that_was_renamed_and_then_dropped_keeps_the_name_it_had()
    {
        var rename = Modify(10, NameAt + 2, [(byte)'A'], [(byte)'B']);
        var drop = new CatalogChange { Lsn = new Lsn(1, 20, 1), TransactionId = 20, Operation = LogOperation.DeleteRows, Table = CatalogTable.Objects, Row0 = Row(Table, 1, "B") };
        var history = CatalogHistory.Build(Now([], new() { [1] = "dbo" }), [rename, drop], Committed(rename, drop), Formats, default);
        Assert.Equal(("dbo", "A"), history.NameAt(Table, new Lsn(1, 5, 1)));
    }

    /// <summary>ALTER SCHEMA s2 TRANSFER s1.t; DROP SCHEMA s1 - before the miner reads that log. What was changed in s1.t is not a change of s2.t.</summary>
    [Fact]
    public void A_table_that_came_from_a_schema_that_is_gone_is_not_taken_for_one_that_was_always_here()
    {
        var transfer = Modify(10, 8, BitConverter.GetBytes(5), BitConverter.GetBytes(6));
        var now = Now(new() { [Table] = new TableIdentity(Table, "s2", "t", new Dictionary<int, string>(), new Dictionary<int, int>()) }, new() { [1] = "dbo", [6] = "s2" });
        var history = CatalogHistory.Build(now, [transfer], Committed(transfer), Formats, default);
        var layout = new TableLayout(5, "db", Table, "s2", "t", 100, DataCompression.None, [Fixed(1, "id", SqlType.Int, 4, 4)]);
        var schema = new DatabaseSchema(5, "db", [layout], new Lsn(1, 30, 1), history: history);
        var before = history.NameAt(Table, new Lsn(1, 5, 1));
        var reported = schema.Renamed(null, default).ToList();
        Assert.True(before is { } name && name.Schema != "s2" || reported.Count > 0,
            $"a change made in schema #5 is published as {before?.Schema ?? "s2"}.t, and {reported.Count} gaps are recorded for it");
    }

    /// <summary>A -> B, rows changed, B -> A (a deployment taken back), read later in one go: the rows changed in between are published as B, and the
    /// consumer of A is told nothing.</summary>
    [Fact]
    public void A_table_that_was_renamed_and_renamed_back_is_reported()
    {
        var there = Modify(10, NameAt + 2, [(byte)'A'], [(byte)'B']);
        var back = Modify(20, NameAt + 2, [(byte)'B'], [(byte)'A']);
        var now = Now(new() { [Table] = new TableIdentity(Table, "dbo", "A", new Dictionary<int, string>(), new Dictionary<int, int>()) }, new() { [1] = "dbo" });
        var history = CatalogHistory.Build(now, [there, back], Committed(there, back), Formats, default);
        var layout = new TableLayout(5, "db", Table, "dbo", "A", 100, DataCompression.None, [Fixed(1, "id", SqlType.Int, 4, 4)]);
        var schema = new DatabaseSchema(5, "db", [layout], new Lsn(1, 30, 1), history: history);
        Assert.Equal(("dbo", "B"), history.NameAt(Table, new Lsn(1, 15, 1))); // the premise: changes in between come out as B
        Assert.NotEmpty(schema.Renamed(null, default));
    }

    private static CatalogHistoryException? Refused(Action a) { try { a(); return null; } catch (CatalogHistoryException ex) { return ex; } }

    private static byte[] B(string x) => System.Runtime.InteropServices.MemoryMarshal.AsBytes(x.AsSpan()).ToArray();
    private static byte[] Aligned(byte[] x) => [.. x, .. new byte[((x.Length + 3) & ~3) - x.Length]];

    /// <summary>The record of a rename, as SQL Server logs it (experiments/38): what changed in one place, or in several: where each was and is, how long it
    /// was, and both contents, each aligned to four.</summary>
    private static CatalogChange Rename(uint n, IReadOnlyList<(string Was, string Is)> pieces, (int Was, int Is)? schema)
    {
        string before = string.Concat(pieces.Select(x => x.Was)), after = string.Concat(pieces.Select(x => x.Is));
        var places = new List<(int From, int To, byte[] Was, byte[] Is)>();
        byte[] endWas = BitConverter.GetBytes((ushort)(NameAt + 2 + 2 * before.Length)), endIs = BitConverter.GetBytes((ushort)(NameAt + 2 + 2 * after.Length));
        if (schema is { } s) places.Add((8, 8, BitConverter.GetBytes(s.Was), BitConverter.GetBytes(s.Is)));
        if (endWas[0] != endIs[0]) places.Add((NameAt, NameAt, [endWas[0]], [endIs[0]]));
        if (endWas[1] != endIs[1]) places.Add((NameAt + 1, NameAt + 1, [endWas[1]], [endIs[1]]));
        int from = NameAt + 2, to = from;
        foreach (var (was, now) in pieces)
        {
            if (was != now)
            {
                // directly behind the end offset that changed it is one place with it
                if (places.Count > 0 && places[^1].From + places[^1].Was.Length == from) places[^1] = (places[^1].From, places[^1].To, [.. places[^1].Was, .. B(was)], [.. places[^1].Is, .. B(now)]);
                else places.Add((from, to, B(was), B(now)));
            }
            from += 2 * was.Length; to += 2 * now.Length;
        }
        if (places.Count == 1 && places[0].From == places[0].To) return new CatalogChange
        {
            Lsn = new Lsn(1, n, 1), TransactionId = n, Operation = LogOperation.ModifyRow, Table = CatalogTable.Objects, Key = Key(Table), Offset = (short)places[0].From, Row0 = places[0].Was, Row1 = places[0].Is,
        };
        return new CatalogChange
        {
            Lsn = new Lsn(1, n, 1), TransactionId = n, Operation = LogOperation.ModifyColumns, Table = CatalogTable.Objects, Key = Key(Table),
            Row0 = [.. places.SelectMany(p => (byte[])[.. BitConverter.GetBytes((short)p.From), .. BitConverter.GetBytes((short)p.To)])],
            Row1 = [.. places.SelectMany(p => BitConverter.GetBytes((short)p.Was.Length))],
            Data = [.. places.SelectMany(p => (byte[])[.. Aligned(p.Was), .. Aligned(p.Is)])],
        };
    }

    /// <summary>Any chain of renames, each in any number of places, is undone to the name of every moment.</summary>
    [HegelTest(TestCases = 1500)]
    public void Any_chain_of_renames_is_undone(TestCase tc)
    {
        var characters = "abcXYZ_09 åö表\ud83d".ToCharArray();
        string Text(string label, int min, int max) => new([.. tc.Draw(Generators.Lists(Generators.SampledFrom(characters)).MinSize(min).MaxSize(max), label)]);
        var name = Text("first name", 1, 40);
        var schema = 5;
        var expected = new List<(string Schema, string Name)> { ("s5", name) };
        var changes = new List<CatalogChange>();
        for (var i = tc.Draw(Generators.Integers().Min(1).Max(30), "renames"); i > 0; i--)
        {
            // pieces that stay and pieces that change, in turn
            var pieces = new List<(string Was, string Is)>();
            var at = 0;
            while (at < name.Length)
            {
                var stays = Math.Min(tc.Draw(Generators.Integers().Min(0).Max(12), "stays"), name.Length - at);
                if (stays == 0 && pieces.Count > 0) stays = 1;
                if (stays > 0) pieces.Add((name.Substring(at, stays), name.Substring(at, stays)));
                at += stays;
                if (!tc.Draw(Generators.Booleans(), "more places")) { if (at < name.Length) pieces.Add((name[at..], name[at..])); break; }
                var goes = Math.Min(tc.Draw(Generators.Integers().Min(0).Max(4), "goes"), name.Length - at);
                var comes = Text("comes", 0, 5);
                pieces.Add((name.Substring(at, goes), comes));
                at += goes;
            }
            var next = string.Concat(pieces.Select(x => x.Is));
            if (next == name || next.Length is 0 or > 120) continue;
            (int, int)? moves = tc.Draw(Generators.Integers().Min(0).Max(4), "and the schema") == 0 ? (schema, 11 - schema) : null;
            if (moves is not null) schema = 11 - schema;
            changes.Add(Rename((uint)(10 * changes.Count + 10), pieces, moves));
            name = next;
            expected.Add(($"s{schema}", name));
        }
        if (changes.Count == 0) return;
        CatalogSnapshot With(int id) => Now(new() { [id] = new TableIdentity(id, $"s{schema}", name, new Dictionary<int, string>(), new Dictionary<int, int>()) }, new() { [5] = "s5", [6] = "s6" });
        var history = CatalogHistory.Build(With(Table), changes, Committed([.. changes]), Formats, default);
        for (var i = 0; i < expected.Count; i++)
            Assert.Equal(expected[i], history.NameAt(Table, new Lsn(1, (uint)(10 * i + 5), 1)) ?? ($"s{schema}", name));
        // of a table that is asked about a record is understood or refused; of another one it does not matter
        if (changes[^1] is not { Operation: LogOperation.ModifyColumns, Data: { Length: > 8 } data } last) return;
        changes[^1] = new CatalogChange { Lsn = last.Lsn, TransactionId = last.TransactionId, Operation = last.Operation, Table = last.Table, Key = last.Key, Row0 = last.Row0, Row1 = last.Row1, Data = data[..^8] };
        Assert.NotNull(Refused(() => CatalogHistory.Build(With(Table), changes, Committed([.. changes]), Formats, default)));
        Assert.Null(Refused(() => CatalogHistory.Build(With(Table + 1), changes, Committed([.. changes]), Formats, default)));
    }
}
