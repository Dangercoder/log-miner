using System.Globalization;
using System.Text;

namespace LogMiner.Verify.Tests;

/// <summary>The choices of one generated history: numbers drawn by Hegel in one piece (a draw is a round trip), read off one by one. Past its
/// end every choice is 0, the plainest one, and nothing rare happens: a shorter or smaller tape is a simpler history.</summary>
internal sealed class Tape(IReadOnlyList<int> values)
{
    private int _at;
    public bool Empty => _at >= values.Count;
    public int Next(int n) => n <= 1 || _at >= values.Count ? 0 : values[_at++] % n;
    public bool Chance(int oneIn) => Next(oneIn) == oneIn - 1;
    public T Pick<T>(IReadOnlyList<T> list) => list[Next(list.Count)];
}

internal enum ColType { Int, BigInt, Decimal, Text, Binary, Bool }

internal sealed class SimCol(string name, ColType type)
{
    public readonly string Name = name;
    public ColType Type = type;
}

/// <summary>One published message, in a form that can be changed and written again.</summary>
internal sealed class Ev
{
    public required SimTable Table;
    public required string Incarnation;
    public required char Op;
    public List<(string Name, string Json)>? Key, Before, After;
    public string Lsn = "";
    public string? CommitLsn, Snapshot, Phase;
    public int Seq = -1;
    public long Rows = -1;
    public string Id = "";
    private byte[]? _payload;

    /// <summary>As the encoder renders it: every part reduced to letters, digits, '_' and '-'.</summary>
    public string Subject => "cdc." + string.Join(".", new[] { Table.Db.Server, Table.Db.Name, Table.Schema, Table.Name }.Select(p => new string([.. p.Select(c => char.IsAsciiLetterOrDigit(c) ? c : '_')])));
    public bool IsChange => Op is 'c' or 'u' or 'd';
    public byte[] Payload => _payload ??= Encoding.UTF8.GetBytes(Render());

    public Ev Clone() => new()
    {
        Table = Table, Incarnation = Incarnation, Op = Op, Key = Key?.ToList(), Before = Before?.ToList(), After = After?.ToList(), Lsn = Lsn, CommitLsn = CommitLsn,
        Snapshot = Snapshot, Phase = Phase, Seq = Seq, Rows = Rows, Id = Id,
    };

    private string Render()
    {
        var sb = new StringBuilder("{\"op\":\"").Append(Op).Append("\",\"key\":");
        Object(sb, Key).Append(",\"before\":");
        Object(sb, Before).Append(",\"after\":");
        Object(sb, After);
        if (Op == 'l')
        {
            sb.Append(",\"load\":{\"id\":").Append(Sim.Quote(Snapshot!)).Append(",\"phase\":\"").Append(Phase).Append('"');
            if (Rows >= 0) sb.Append(",\"rows\":").Append(Rows);
            sb.Append('}');
        }
        sb.Append(",\"source\":{\"server\":").Append(Sim.Quote(Table.Db.Server)).Append(",\"db\":").Append(Sim.Quote(Table.Db.Name)).Append(",\"incarnation\":").Append(Sim.Quote(Incarnation))
            .Append(",\"schema\":").Append(Sim.Quote(Table.Schema)).Append(",\"table\":").Append(Sim.Quote(Table.Name)).Append(",\"lsn\":\"").Append(Lsn).Append('"');
        if (CommitLsn is not null) sb.Append(",\"commit_lsn\":\"").Append(CommitLsn).Append("\",\"xid\":\"0000:0000002a\",\"seq\":").Append(Seq).Append(",\"txn_size\":3,\"commit_time\":\"2026-11-25T10:00:00.000\"");
        if (Snapshot is not null) sb.Append(",\"snapshot\":").Append(Sim.Quote(Snapshot));
        return sb.Append("},\"ts_ms\":1795600000000}").ToString();
    }

    private static StringBuilder Object(StringBuilder sb, List<(string Name, string Json)>? image)
    {
        if (image is null) return sb.Append("null");
        sb.Append('{');
        for (var i = 0; i < image.Count; i++) sb.Append(i > 0 ? "," : "").Append(Sim.Quote(image[i].Name)).Append(':').Append(image[i].Json);
        return sb.Append('}');
    }

    public override string ToString() => $"{Id} {Encoding.UTF8.GetString(Payload)}";
}

internal sealed class SimLoad
{
    public required string Id;
    /// <summary>The last key read; null before the first chunk.</summary>
    public string? Cursor, CursorBefore;
    /// <summary>Rows read and not yet published, and the keys changed since they were read.</summary>
    public List<(string Key, List<(string, string)> KeyObject, List<(string, string)> Image)>? Chunk;
    public readonly HashSet<string> Touched = new(StringComparer.Ordinal);
    public bool ReadAll;
    public long Published;
    public int Attempt, Reads;
}

internal sealed class SimTable
{
    public required SimDb Db;
    public required string Schema, Name;
    public required List<SimCol> Cols;
    /// <summary>Empty: a table without a key.</summary>
    public required List<string> KeyCols;
    public required int KeyKind;
    /// <summary>The table as it is, by key (the key columns' JSON); a keyless table's rows have a number.</summary>
    public SortedDictionary<string, Dictionary<string, string>> Rows = new(StringComparer.Ordinal);
    public SimLoad? Load;
    public int NextCol, NextLoad, NextRow;

    public bool Keyed => KeyCols.Count > 0;
    public string KeyOf(Dictionary<string, string> row) => string.Join("\u0001", KeyCols.Select(k => row[k]));
}

internal sealed class SimDb
{
    public required string Server, Name;
    public string Incarnation = "";
    public uint Vlf = 0x2A, Block = 0x10;
    public int Incarnations, Marker;
    public readonly List<SimTable> Tables = [];
    /// <summary>The changes published in this incarnation: what a restarted miner sends again from some point.</summary>
    public readonly List<Ev> Published = [];
    /// <summary>Everything published for the database, for a delivery that repeats its last messages.</summary>
    public readonly List<Ev> Emitted = [];
    public string Lsn => string.Create(CultureInfo.InvariantCulture, $"{Vlf:X8}:{Block:X8}:0001");
}

/// <summary>
/// Tables that change, and the stream a miner publishes about them, exactly under its guarantees: changes in commit order with full images;
/// historical loads between a begin and an end marker, read in chunks while changes go on, a chunk's rows published at a later place in the
/// stream unless something changed them meanwhile; loads that are interrupted and start over; a miner that restarts and sends changes again
/// from an earlier point; a delivery that repeats its last messages, whatever they are; DDL without events; databases that come back as
/// another incarnation.
/// </summary>
internal sealed class Sim
{
    public readonly List<SimDb> Dbs = [];
    public readonly List<Ev> Out = [];
    private Tape _t;
    /// <summary>The last messages may be delivered again as they were (needs message ids to be passed over).</summary>
    public bool Redelivery = true;
    /// <summary>Rows of loads carry a commit_lsn and seq, as they used to: far below or far above every change. They mean nothing.</summary>
    public bool RowsCarryPositions;
    public readonly Dictionary<string, int> Seen = new(StringComparer.Ordinal);
    private void Cover(string what) => Seen[what] = Seen.GetValueOrDefault(what) + 1;

    private static readonly string[] Ints = ["0", "1", "2", "3", "-1", "7", "2147483647"];
    private static readonly string[] BigInts = ["9007199254740992", "9007199254740993", "9223372036854775807", "-9223372036854775808", "0", "9007199254740991"];
    private static readonly string[] Decimals = ["\"0.10\"", "\"12.50\"", "\"99999999999999999999.999999\"", "\"7\"", "\"-3\"", "\"12.5\""];
    // "a"/"A" differ in case only; half a surrogate pair arrives as an escape; "1" is not 1; a long one is kept as a hash
    private static readonly string[] Texts = ["\"\"", "\"a\"", "\"A\"", "\"å\"", "\"abc\"", "\"😀\"", "\"\\ud83d\"", "\"\\ude00\"", "\"1\"", "\"the quick brown fox jumps over the lazy dog\"", "\"the quick brown fox jumps over the lazy dot\"", "\"a\\nb\"", "\"ab\\\"c\\\\\""];
    private static readonly string[] Binaries = ["\"\"", "\"AA==\"", "\"3q2+7w==\"", "\"AAECAwQFBgcICQoLDA0ODw==\"", "\"AAECAwQFBgcICQoLDA0OEA==\""];
    private static readonly string[][] KeyValues =
    [
        ["0", "1", "2", "3", "4", "5"],
        ["\"a\"", "\"A\"", "\"b\"", "\"1\"", "\"å\"", "\"\\ud83d\"", "\"\\ude00\""],
        ["1", "\"1\"", "\"a\"", "\"A\"", "2", "1.0"], // of any type (sql_variant): 1, "1" and 1.0 are three keys
        ["null", "\"a\"", "\"A\"", "\"\""], // a unique clustered index on a nullable column: NULL is a key
    ];

    public Sim(Tape tape)
    {
        _t = tape;
        var dbs = 1 + _t.Next(2);
        for (var d = 0; d < dbs; d++)
        {
            // two databases of one name on two servers, or two names on one
            var db = new SimDb { Server = d == 1 && _t.Chance(2) ? "srv B" : "srv", Name = d == 0 ? "db" : _t.Chance(2) ? "db" : "Ünï \"db\"" };
            if (d == 1 && db.Server == "srv" && db.Name == "db") db.Name = "dB"; // another database: names differ in case only
            db.Incarnation = "06E4" + d;
            Dbs.Add(db);
            var tables = 1 + _t.Next(3);
            for (var i = 0; i < tables; i++) db.Tables.Add(NewTable(db, i));
        }
        // most tables are seen from their very first load; of the others the stream knows only what changed
        foreach (var t in Dbs.SelectMany(d => d.Tables))
            if (_t.Next(3) != 0) { FullLoad(t); Cover("loaded-first"); }
    }

    private SimTable NewTable(SimDb db, int n)
    {
        var kind = _t.Next(6); // 0 int, 1 text, 2 any type, 3 nullable, 4 two columns, 5 no key
        var t = new SimTable { Db = db, Schema = n == 2 ? "s 2" : "dbo", Name = n == 0 ? "t" : n == 1 ? "T" : "t" + n, Cols = [], KeyCols = [], KeyKind = kind };
        if (kind == 4) { t.Cols.Add(new SimCol("id", ColType.Int)); t.Cols.Add(new SimCol("Id", ColType.Text)); t.KeyCols.AddRange(["id", "Id"]); }
        else if (kind != 5) { t.Cols.Add(new SimCol("id", kind == 0 ? ColType.Int : ColType.Text)); t.KeyCols.Add("id"); }
        var cols = 1 + _t.Next(4);
        for (var i = 0; i < cols; i++) t.Cols.Add(new SimCol("c" + t.NextCol++, (ColType)_t.Next(6)));
        // rows that were there before the stream begins: only a load can tell
        for (var rows = _t.Next(5); rows > 0; rows--)
            if (NewKey(t) is { } key) Insert(t, key);
        return t;
    }

    // ---- values --------------------------------------------------------------------------------------------------------------------

    private string Value(ColType type) => _t.Chance(6) ? "null" : type switch
    {
        ColType.Int => _t.Pick(Ints), ColType.BigInt => _t.Pick(BigInts), ColType.Decimal => _t.Pick(Decimals), ColType.Text => _t.Pick(Texts),
        ColType.Binary => _t.Pick(Binaries), _ => _t.Chance(2) ? "true" : "false",
    };

    /// <summary>A key the table does not have, as its columns' values; null when every key is taken.</summary>
    private List<string>? NewKey(SimTable t)
    {
        if (!t.Keyed) return [];
        for (var attempt = 0; attempt < 8; attempt++)
        {
            List<string> key = t.KeyKind == 4 ? [_t.Pick(KeyValues[0][..3]), _t.Pick(KeyValues[1][..3])] : [_t.Pick(KeyValues[t.KeyKind])];
            if (!t.Rows.ContainsKey(string.Join("\u0001", key))) return key;
        }
        return null;
    }

    private Dictionary<string, string> Insert(SimTable t, List<string> key)
    {
        var row = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var c in t.Cols) row[c.Name] = t.KeyCols.IndexOf(c.Name) is >= 0 and var k ? key[k] : Value(c.Type);
        t.Rows.Add(t.Keyed ? t.KeyOf(row) : (t.NextRow++).ToString("D6", CultureInfo.InvariantCulture), row);
        return row;
    }

    /// <summary>The same string as another JSON text: every character beyond ASCII as an escape. A reader must take both for the same value.</summary>
    private string Spelled(string json)
    {
        if (json.Length < 2 || json[0] != '"' || json.All(c => c < 0x80) || !_t.Chance(5)) return json;
        var sb = new StringBuilder();
        foreach (var c in json) sb.Append(c < 0x80 ? c.ToString() : "\\u" + ((int)c).ToString("X4", CultureInfo.InvariantCulture));
        return sb.ToString();
    }

    public static string Quote(string s)
    {
        var sb = new StringBuilder("\"");
        foreach (var c in s) sb.Append(c is '"' or '\\' ? "\\" + c : c.ToString());
        return sb.Append('"').ToString();
    }

    private List<(string, string)> Image(SimTable t, Dictionary<string, string> row, int unavailable) =>
        [.. t.Cols.Select(c => (c.Name, !t.KeyCols.Contains(c.Name) && unavailable > 0 && _t.Chance(unavailable) ? Unavailable() : Spelled(row[c.Name])))];

    private string Unavailable() { Cover("unavailable"); return "{\"unavailable\":true}"; }

    private List<(string, string)>? KeyObject(SimTable t, Dictionary<string, string> row) => t.Keyed ? [.. t.KeyCols.Select(k => (k, Spelled(row[k])))] : null;

    // ---- publishing ----------------------------------------------------------------------------------------------------------------

    private void Emit(Ev e)
    {
        Out.Add(e);
        e.Table.Db.Emitted.Add(e);
        if (e.IsChange) e.Table.Db.Published.Add(e);
    }

    private string NextLsn(SimDb db)
    {
        // positions order by all their parts: a new virtual log file starts its blocks low again
        if (_t.Chance(8)) { db.Vlf++; db.Block = (uint)_t.Next(12); Cover("new-vlf"); }
        else db.Block += 1 + (uint)_t.Next(3);
        return db.Lsn;
    }

    private void Marker(SimTable t, string phase, long rows = -1)
    {
        var db = t.Db;
        Emit(new Ev { Table = t, Incarnation = db.Incarnation, Op = 'l', Lsn = db.Lsn, Snapshot = t.Load!.Id, Phase = phase, Rows = rows, Id = $"{db.Server}:{db.Name}:{db.Incarnation}:load:{t.Load.Id}:{phase}:{db.Marker++}" });
    }

    // ---- steps ---------------------------------------------------------------------------------------------------------------------

    /// <summary>Goes on with other choices.</summary>
    public void Use(Tape tape) => _t = tape;

    public void Run(int steps = int.MaxValue)
    {
        for (var i = 0; i < steps && !_t.Empty; i++) Step();
    }

    public void Step()
    {
        var db = _t.Pick(Dbs);
        var t = _t.Pick(db.Tables);
        switch (_t.Next(24))
        {
            case 10 or 11 or 12 when t.Load is null: Begin(t); break;
            case 10 or 11 or 12 or 13 or 14 or 15 when t.Load is not null: Advance(t); break;
            case 16: Ddl(t); break;
            case 17: Restart(db); break;
            case 18 when Redelivery: Redeliver(db); break;
            case 19 when t.Load is not null: StartOver(t); Cover("load-starts-over"); break;
            case 20 when _t.Chance(3): NewIncarnation(db); break;
            default: Transaction(db); break;
        }
    }

    private void Transaction(SimDb db)
    {
        var commit = NextLsn(db);
        var seq = _t.Next(2);
        var events = new List<Ev>();
        for (var ops = 1 + _t.Next(4); ops > 0; ops--)
        {
            var t = _t.Pick(db.Tables);
            var keys = t.Rows.Keys.ToList();
            var op = _t.Next(10);
            if (op is 6 or 7 && keys.Count >= 2 && t.Keyed)
            {
                // two rows of a heap change keys in one statement: the first gets a key the second still has. Swapped, or shifted along
                var (a, b) = (t.Rows[keys[_t.Next(keys.Count)]], t.Rows[keys[_t.Next(keys.Count)]]);
                if (ReferenceEquals(a, b)) continue;
                var third = op == 7 ? NewKey(t) : null;
                var (ka, kb) = (t.KeyCols.Select(k => a[k]).ToList(), t.KeyCols.Select(k => b[k]).ToList());
                t.Rows.Remove(t.KeyOf(a));
                t.Rows.Remove(t.KeyOf(b));
                events.Add(Update(t, a, kb, commit, ref seq));
                events.Add(Update(t, b, third ?? ka, commit, ref seq));
                t.Rows.Add(t.KeyOf(a), a);
                t.Rows.Add(t.KeyOf(b), b);
                Cover(third is null ? "keys-swapped" : "keys-shifted");
            }
            else if (op is 0 or 1 or 2 || keys.Count == 0)
            {
                if (NewKey(t) is not { } key) continue;
                var row = Insert(t, key);
                Touch(t, row);
                events.Add(new Ev { Table = t, Incarnation = db.Incarnation, Op = 'c', Key = KeyObject(t, row), After = Image(t, row, 12), Lsn = commit, CommitLsn = commit, Seq = seq });
                Cover(t.Keyed ? "insert" : "insert-without-key");
            }
            else if (op is 3)
            {
                var key = _t.Pick(keys);
                var row = t.Rows[key];
                Touch(t, row);
                t.Rows.Remove(key);
                events.Add(new Ev { Table = t, Incarnation = db.Incarnation, Op = 'd', Key = KeyObject(t, row), Before = Image(t, row, 12), Lsn = commit, CommitLsn = commit, Seq = seq });
                Cover("delete");
            }
            else if (op is 4 && t.Keyed && NewKey(t) is { } moved)
            {
                var row = t.Rows[_t.Pick(keys)];
                t.Rows.Remove(t.KeyOf(row));
                events.Add(Update(t, row, moved, commit, ref seq));
                t.Rows.Add(t.KeyOf(row), row);
                Cover("key-changed");
                continue;
            }
            else
            {
                events.Add(Update(t, t.Rows[_t.Pick(keys)], null, commit, ref seq));
                Cover("update");
                continue;
            }
            seq += 1 + _t.Next(2); // not contiguous: changes to rows that are not captured have numbers, too
        }
        foreach (var e in events) e.Id = $"{db.Server}:{db.Name}:{db.Incarnation}:{e.CommitLsn}:{e.Seq}";
        // the miner may stop in the middle of publishing a transaction: it comes back, sends again from an earlier point, and goes on
        var cut = events.Count >= 2 && _t.Chance(8) ? 1 + _t.Next(events.Count - 1) : -1;
        for (var i = 0; i < events.Count; i++)
        {
            if (i == cut) { Restart(db); Cover("restart-inside-transaction"); }
            Emit(events[i]);
        }
        if (events.Count > 1) Cover("transaction-of-several");
    }

    /// <summary>An update of a row: of some of its values, and of its key when it gets a new one (the caller files the row under it).</summary>
    private Ev Update(SimTable t, Dictionary<string, string> row, List<string>? newKey, string commit, ref int seq)
    {
        var before = Image(t, row, 12);
        Touch(t, row);
        for (var k = 0; newKey is not null && k < t.KeyCols.Count; k++) row[t.KeyCols[k]] = newKey[k];
        var others = t.Cols.Where(c => !t.KeyCols.Contains(c.Name)).ToList();
        // sometimes nothing changes (SET x = x): an update all the same
        for (var n = _t.Next(3); n > 0 && others.Count > 0; n--) { var c = _t.Pick(others); row[c.Name] = Value(c.Type); }
        Touch(t, row);
        var e = new Ev { Table = t, Incarnation = t.Db.Incarnation, Op = 'u', Key = KeyObject(t, row), Before = before, After = Image(t, row, 12), Lsn = commit, CommitLsn = commit, Seq = seq };
        seq += 1 + _t.Next(2);
        return e;
    }

    /// <summary>A chunk that was read and is not published yet must not publish a row that changed since.</summary>
    private static void Touch(SimTable t, Dictionary<string, string> row)
    {
        if (t.Keyed && t.Load?.Chunk is not null) t.Load.Touched.Add(t.KeyOf(row));
    }

    private void Begin(SimTable t)
    {
        t.Load = new SimLoad { Id = $"L{t.NextLoad++}" };
        Marker(t, "begin");
        Cover("load-begins");
    }

    private void StartOver(SimTable t)
    {
        var load = t.Load!;
        (load.Cursor, load.Chunk, load.ReadAll, load.Published) = (null, null, false, 0);
        load.Touched.Clear();
        load.Attempt++;
        Marker(t, "begin");
    }

    /// <summary>Reads the next chunk, or publishes the one that was read, or ends the load.</summary>
    private void Advance(SimTable t)
    {
        var load = t.Load!;
        if (load.Chunk is not null)
        {
            // the stream has reached the place the chunk was read at: its rows, unless something changed them since (that change is in the stream)
            var n = 0;
            // a fault while the chunk is sent: some of its rows are in the stream, none of them counts, and the chunk is read again later
            // (other message ids, the rows as they are then). The load announces the rows of the chunks that went through
            var fault = _t.Chance(7) ? _t.Next(load.Chunk.Count + 1) : -1;
            foreach (var (key, keyObject, image) in load.Chunk)
            {
                if (n == fault) break;
                if (load.Touched.Contains(key)) { Cover("chunk-row-dropped"); continue; }
                var e = new Ev { Table = t, Incarnation = t.Db.Incarnation, Op = 'r', Key = t.Keyed ? keyObject : null, After = image, Lsn = t.Db.Lsn, Snapshot = load.Id, Id = $"{t.Db.Server}:{t.Db.Name}:{t.Db.Incarnation}:snap:{t.Schema}.{t.Name}.{load.Id}.{load.Attempt}.{load.Reads}:{n++}" };
                if (RowsCarryPositions) (e.CommitLsn, e.Seq) = (_t.Chance(2) ? "FFFFFFF0:00000001:0001" : "00000001:00000001:0001", n);
                Emit(e);
                if (fault < 0) load.Published++;
                Cover("load-row");
            }
            if (fault >= 0) { (load.Cursor, load.ReadAll) = (load.CursorBefore, false); Cover("chunk-published-again"); }
            load.Chunk = null;
            load.Touched.Clear();
        }
        else if (!load.ReadAll)
        {
            var size = 1 + _t.Next(3);
            var rows = t.Rows.Where(r => load.Cursor is null || string.CompareOrdinal(r.Key, load.Cursor) > 0).Take(size).ToList();
            load.Chunk = [.. rows.Select(r => (r.Key, KeyObject(t, r.Value) ?? [], Image(t, r.Value, 40)))];
            load.Reads++;
            load.CursorBefore = load.Cursor;
            if (rows.Count > 0) load.Cursor = rows[^1].Key;
            if (rows.Count < size) load.ReadAll = true;
        }
        else
        {
            Marker(t, "end", load.Published);
            t.Load = null;
            Cover("load-ends");
        }
    }

    /// <summary>A whole load with nothing going on meanwhile.</summary>
    public void FullLoad(SimTable t)
    {
        if (t.Load is null) Begin(t); else StartOver(t);
        while (t.Load is not null) Advance(t);
    }

    /// <summary>DDL publishes nothing. Column names are not used twice (see the report: a column dropped and added again cannot be told from a
    /// lost update), and a type change keeps the value (one that rounds changes rows without an event).</summary>
    private void Ddl(SimTable t)
    {
        var others = t.Cols.Where(c => !t.KeyCols.Contains(c.Name)).ToList();
        switch (_t.Next(3))
        {
            case 0:
                var added = new SimCol("c" + t.NextCol++, (ColType)_t.Next(6));
                t.Cols.Insert(_t.Next(t.Cols.Count + 1), added);
                var value = Value(added.Type); // a default: every row has it at once
                foreach (var row in t.Rows.Values) row[added.Name] = value;
                Cover("column-added");
                break;
            case 1 when others.Count > 1:
                var dropped = _t.Pick(others);
                t.Cols.Remove(dropped);
                foreach (var row in t.Rows.Values) row.Remove(dropped.Name);
                Cover("column-dropped");
                break;
            default:
                // number <-> string: int to varchar or decimal and back while every value still is a whole number
                var c = _t.Pick(t.Cols);
                if (t.KeyCols.Contains(c.Name)) break;
                var values = t.Rows.Values.Select(r => r[c.Name]).Where(v => v != "null").ToList();
                if (c.Type is ColType.Int or ColType.BigInt)
                {
                    c.Type = _t.Chance(2) ? ColType.Text : ColType.Decimal;
                    foreach (var row in t.Rows.Values) if (row[c.Name] != "null") row[c.Name] = $"\"{row[c.Name]}\"";
                    Cover("column-retyped");
                }
                else if (c.Type is ColType.Text or ColType.Decimal && values.Count > 0 && values.All(v => long.TryParse(v.Trim('"'), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var n) && n.ToString(CultureInfo.InvariantCulture) == v.Trim('"')))
                {
                    c.Type = ColType.BigInt;
                    foreach (var row in t.Rows.Values) if (row[c.Name] != "null") row[c.Name] = row[c.Name].Trim('"');
                    Cover("column-retyped-back");
                }
                break;
        }
    }

    /// <summary>The miner restarts: changes it had published come again, in order, from some earlier point; its loads start over.</summary>
    private void Restart(SimDb db)
    {
        var loads = db.Tables.Where(t => t.Load is not null).ToList();
        var first = _t.Chance(2);
        if (first) foreach (var t in loads) StartOver(t);
        var back = Math.Min(db.Published.Count, _t.Next(10));
        for (var i = db.Published.Count - back; i < db.Published.Count; i++) Out.Add(db.Published[i]);
        if (!first) foreach (var t in loads) StartOver(t);
        Cover(back > 0 ? "replay" : "restart");
        if (loads.Count > 0) Cover("restart-during-load");
    }

    /// <summary>The last messages are delivered once more as they were, rows of loads and markers among them: the same message ids.</summary>
    private void Redeliver(SimDb db)
    {
        var back = Math.Min(db.Emitted.Count, 1 + _t.Next(8));
        for (var i = db.Emitted.Count - back; i < db.Emitted.Count; i++)
        {
            Out.Add(db.Emitted[i]);
            if (!db.Emitted[i].IsChange) Cover("load-message-redelivered");
        }
    }

    /// <summary>The database is restored: other rows, positions that start over, loads that are gone.</summary>
    private void NewIncarnation(SimDb db)
    {
        db.Incarnation = $"06E4{Dbs.IndexOf(db)}-{++db.Incarnations}";
        (db.Vlf, db.Block) = (0x20, 3);
        db.Published.Clear();
        db.Emitted.Clear();
        foreach (var t in db.Tables)
        {
            t.Load = null;
            foreach (var key in t.Rows.Keys.ToList())
            {
                if (_t.Chance(2)) t.Rows.Remove(key);
                else foreach (var c in t.Cols.Where(c => !t.KeyCols.Contains(c.Name) && _t.Chance(3))) t.Rows[key][c.Name] = Value(c.Type);
            }
            if (_t.Chance(2) && NewKey(t) is { } added) Insert(t, added);
        }
        Cover("incarnation");
    }

    /// <summary>Replays and repeated deliveries reach back no further than this point.</summary>
    public void Forget()
    {
        foreach (var db in Dbs) { db.Published.Clear(); db.Emitted.Clear(); }
    }

    /// <summary>Every load that is running ends, then every table is loaded once more, with nothing going on: the place where anything that
    /// is wrong must have shown. Returns where those last loads begin in <see cref="Out"/>.</summary>
    public int Settle()
    {
        foreach (var t in Dbs.SelectMany(d => d.Tables).Where(t => t.Load is not null)) FullLoad(t);
        var at = Out.Count;
        foreach (var t in Dbs.SelectMany(d => d.Tables)) FullLoad(t);
        return at;
    }
}
