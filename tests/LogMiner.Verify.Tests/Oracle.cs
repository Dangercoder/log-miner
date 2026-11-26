using System.Globalization;
using System.Text;

namespace LogMiner.Verify.Tests;

/// <summary>
/// What anybody who follows the stream can know: a plain replica, built the way the design tells consumers to (changes above the database's
/// high-water mark applied in order, rows of a load upserted, a repeated message id passed over), with whole values in dictionaries - no
/// hashes, no compact rows, no epochs. It decides which damage to a stream can be seen at all:
/// <list type="bullet">
/// <item>a later message contradicts what the replica holds for its row (a before image, a row of a load, an insert onto a row that is there,
/// a change or a load's row for a key a complete replica lacks), or</item>
/// <item>the replica is not the table when the next complete load begins.</item>
/// </list>
/// Everything else nobody can see, and the verifier is not asked to (see <see cref="StreamVerifierProperties.Nothing_is_missed"/>).
/// </summary>
internal sealed class Oracle
{
    private const string Unknown = "\u0000unknown";

    private sealed class Table
    {
        /// <summary>By key. More than one row on a key only inside a transaction (two rows of a heap exchanging keys).</summary>
        public readonly Dictionary<string, List<Dictionary<string, string>>> Rows = new(StringComparer.Ordinal);
        public readonly HashSet<string> Loads = new(StringComparer.Ordinal);
        public bool Complete;
    }

    private sealed class Db
    {
        public string? Incarnation;
        public (string Commit, int Seq)? Position;
        public string? Transaction;
        public readonly HashSet<string> Ids = new(StringComparer.Ordinal);
        public readonly HashSet<(Table, string)> Touched = [];
        /// <summary>Keys a row moved onto in this transaction that nothing was known about: a row may have been there, and leave next.</summary>
        public readonly HashSet<(Table, string)> OntoUnknown = [];
    }

    private readonly Dictionary<SimDb, Db> _dbs = [];
    private readonly Dictionary<SimTable, Table> _tables = [];
    /// <summary>The first thing that showed, or null.</summary>
    public string? Seen { get; private set; }
    public bool ByChain { get; private set; }

    private void See(string what, bool chain)
    {
        if (Seen is not null) return;
        (Seen, ByChain) = (what, chain);
    }

    /// <summary>A JSON value as what it means: 1 and "1" differ, "å" and "å" do not, half a surrogate pair is itself.</summary>
    public static string Canonical(string json)
    {
        if (json.StartsWith('{')) return Unknown;
        if (!json.StartsWith('"')) return json;
        var sb = new StringBuilder("\"");
        for (var i = 1; i < json.Length - 1; i++)
        {
            if (json[i] != '\\') { sb.Append(json[i]); continue; }
            var c = json[++i];
            if (c == 'u') { sb.Append((char)int.Parse(json.AsSpan(i + 1, 4), NumberStyles.HexNumber, CultureInfo.InvariantCulture)); i += 4; }
            else sb.Append(c switch { 'n' => '\n', 't' => '\t', 'r' => '\r', 'b' => '\b', 'f' => '\f', _ => c });
        }
        return sb.ToString();
    }

    private static char Class(string canonical) => canonical[0] switch { '"' => 's', 'n' => 'n', 't' or 'f' => 'b', _ => '1' };

    /// <summary>Do two values of a column that both sides know say different things? A value of another JSON kind is a column that was retyped.</summary>
    public static bool Contradict(string a, string b) =>
        a != Unknown && b != Unknown && a != b && (Class(a) == Class(b) || Class(a) == 'n' || Class(b) == 'n');

    private static bool Contradict(Dictionary<string, string> row, Dictionary<string, string> image) =>
        row.Any(c => image.TryGetValue(c.Key, out var v) && Contradict(c.Value, v));

    private static Dictionary<string, string> Row(List<(string Name, string Json)> image) => image.ToDictionary(c => c.Name, c => Canonical(c.Json), StringComparer.Ordinal);
    private static string Key(IEnumerable<string> values) => string.Join("\u0001", values.Select(Canonical));

    public void Add(Ev e)
    {
        if (!_dbs.TryGetValue(e.Table.Db, out var db)) _dbs.Add(e.Table.Db, db = new Db());
        if (!_tables.TryGetValue(e.Table, out var t)) _tables.Add(e.Table, t = new Table());
        if (!e.IsChange && !db.Ids.Add(e.Id)) return; // delivered again
        if (db.Incarnation != e.Incarnation)
        {
            // another database under the name: nothing known holds
            foreach (var other in _tables.Where(x => x.Key.Db == e.Table.Db)) { other.Value.Rows.Clear(); other.Value.Loads.Clear(); other.Value.Complete = false; }
            (db.Incarnation, db.Position, db.Transaction) = (e.Incarnation, null, null);
            db.Touched.Clear();
            db.OntoUnknown.Clear();
        }
        if (e.Op == 'l')
        {
            if (e.Phase == "begin") t.Loads.Add(e.Snapshot!);
            else { EndTransaction(db); if (t.Loads.Remove(e.Snapshot!)) t.Complete = true; }
            return;
        }
        if (e.Op == 'r')
        {
            EndTransaction(db);
            if (e.Key is null) return;
            var (key, row) = (Key(e.Key.Select(k => k.Json)), Row(e.After!));
            if (t.Rows.TryGetValue(key, out var has))
            {
                if (Contradict(has[0], row)) See($"a row of a load differs: {e}", chain: false);
            }
            else if (t.Complete) See($"a row of a load that a complete replica lacks: {e}", chain: false);
            t.Rows[key] = [row];
            return;
        }
        var position = (e.CommitLsn!, e.Seq);
        if (db.Position is { } last && (string.CompareOrdinal(position.Item1, last.Commit) < 0 || (position.Item1 == last.Commit && position.Seq <= last.Seq))) return;
        if (db.Transaction != e.CommitLsn) EndTransaction(db);
        (db.Transaction, db.Position) = (e.CommitLsn, position);
        if (e.Key is null) return;
        var names = e.Key.Select(k => k.Name).ToList();
        if (e.Before is not null)
        {
            var before = Row(e.Before);
            var kb = Key(names.Select(n => e.Before.First(c => c.Name == n).Json));
            if (t.Rows.TryGetValue(kb, out var rows) && rows.Count > 0)
            {
                // the row this is the before image of; if none is, the stream said something else about this row before
                var which = rows.FindIndex(r => !Contradict(r, before));
                if (which < 0 && db.OntoUnknown.Remove((t, kb))) which = -2; // the row that was there unseen
                else if (which < 0) { See($"a before image differs: {e}", chain: true); which = rows.Count - 1; }
                if (which >= 0) rows.RemoveAt(which);
                if (rows.Count == 0) t.Rows.Remove(kb);
            }
            else if (t.Complete) See($"a change of a row that a complete replica lacks: {e}", chain: true);
            else db.OntoUnknown.Remove((t, kb));
        }
        if (e.After is not null)
        {
            var ka = Key(e.Key.Select(k => k.Json));
            if (!t.Rows.TryGetValue(ka, out var rows))
            {
                t.Rows.Add(ka, rows = []);
                if (!t.Complete && e.Op == 'u') db.OntoUnknown.Add((t, ka));
            }
            rows.Add(Row(e.After));
            db.Touched.Add((t, ka));
        }
    }

    /// <summary>When a transaction is over, a key has one row: two means that the one that was there was deleted without the stream saying so.</summary>
    private void EndTransaction(Db db)
    {
        foreach (var (t, key) in db.Touched)
        {
            if (!t.Rows.TryGetValue(key, out var rows) || rows.Count < 2) continue;
            See($"a row arrived on key {key} that had one", chain: true);
            rows.RemoveRange(0, rows.Count - 1);
        }
        db.Touched.Clear();
        db.OntoUnknown.Clear();
        db.Transaction = null;
    }

    /// <summary>Is the replica the table? Asked where a load of every table begins: if not, that load must show it.</summary>
    public void CompareWithTables(IEnumerable<SimTable> tables)
    {
        // (a transaction that was the last of its incarnation never ended in the stream: what it left open is not known to be wrong)
        foreach (var (db, known) in _dbs)
            if (known.Incarnation == db.Incarnation) EndTransaction(known);
        foreach (var table in tables.Where(t => t.Keyed))
        {
            _tables.TryGetValue(table, out var t);
            if (t is null || _dbs[table.Db].Incarnation != table.Db.Incarnation) continue; // nothing known of it as it is now
            var truth = table.Rows.Values.ToDictionary(r => Key(table.KeyCols.Select(k => r[k])), r => r.ToDictionary(c => c.Key, c => Canonical(c.Value), StringComparer.Ordinal), StringComparer.Ordinal);
            foreach (var (key, rows) in t.Rows)
            {
                if (!truth.TryGetValue(key, out var row)) See($"the replica has a row {key} that {table.Name} does not have", chain: false);
                else if (Contradict(rows[0], row)) See($"the replica's row {key} is not that of {table.Name}", chain: false);
            }
            if (t.Complete && truth.Keys.FirstOrDefault(k => !t.Rows.ContainsKey(k)) is { } missing) See($"a complete replica lacks row {missing} of {table.Name}", chain: false);
        }
    }
}
