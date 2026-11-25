using System.Globalization;
using System.IO.Hashing;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace LogMiner.Verify;

public sealed class VerifierOptions
{
    /// <summary>Examples kept per table and kind of difference.</summary>
    public int Examples { get; init; } = 5;
    /// <summary>Message ids of load rows and markers remembered per database: one that comes again within so many is a repeat (delivery is
    /// at-least-once, and a row of a load has no position to tell by). 0: every load message counts.</summary>
    public int ReplayWindow { get; init; } = 100_000;
    /// <summary>Compare the number of rows a load announces at its end with the rows that arrived.</summary>
    public bool CheckLoadRows { get; init; } = true;
}

/// <summary>
/// Proves from the published stream alone that it matches its tables. It follows the stream as a consumer would, keeps every row of every
/// table (compactly: <see cref="RowStore"/>), and checks two things.
/// <para><b>Chain.</b> The before image of an update or delete must be the row as the stream left it, an inserted key must not be there. A
/// change that was lost shows with the next change to its row.</para>
/// <para><b>Load.</b> A historical load publishes the table's rows at their place in the stream, between a begin and an end marker. Every row
/// of it must be the row the stream has built; at the end, what the state has and the load neither read nor saw changed is not in the table;
/// and once a state is complete (a load has ended), a row the load reads and the state lacks was never inserted in the stream. So a load
/// compares the whole table with the stream without anybody reading the table a second time.</para>
/// <para>After a difference the state takes what the stream says now, so one loss is reported once. Columns only one side has (DDL) are not
/// compared; a value that is unavailable on either side matches; a value whose JSON kind changed counts as retyped. Changes at or below the
/// database's high-water mark of (commit_lsn, seq) are repeats and passed over; rows of a load never touch that mark. Not thread-safe: the
/// stream has one order.</para>
/// </summary>
public sealed class StreamVerifier(VerifierOptions? options = null)
{
    private readonly VerifierOptions _options = options ?? new VerifierOptions();
    private readonly Dictionary<byte[], TableState> _tables = new(Bytes.Comparer);
    private readonly Dictionary<byte[], DbState> _dbs = new(Bytes.Comparer);
    private readonly List<TableState> _order = [];
    private readonly List<string> _notes = [], _unreadableExamples = [];
    private readonly List<(int State, int Image)> _differing = [];
    private readonly Image _a = new(), _b = new();
    private TableState? _last;
    private byte[] _id = new byte[256], _scratch = new byte[256], _ka = new byte[64], _kb = new byte[64];
    private int[] _map = new int[32], _keyColumns = new int[8];
    private long _messages, _unreadable, _unknown;
    private ulong _firstSequence, _lastSequence;
    private DateTimeOffset? _firstTime;

    /// <summary>Called for every difference as it is found.</summary>
    public Action<Difference>? OnDifference { get; set; }

    public long Messages => _messages;
    /// <summary>Differences so far, in all tables.</summary>
    public long Differences { get; private set; }
    public long Unreadable => _unreadable;

    /// <summary>The next message of the stream, with its headers (Nats-Msg-Id tells a repeated row of a load).</summary>
    public void Add(string subject, IEnumerable<KeyValuePair<string, string>>? headers, ReadOnlySpan<byte> payload, ulong sequence = 0, DateTimeOffset time = default)
    {
        string? id = null;
        if (headers is not null)
            foreach (var h in headers)
                if (h.Key.Equals("Nats-Msg-Id", StringComparison.OrdinalIgnoreCase)) { id = h.Value; break; }
        Add(subject, payload, id, sequence, time);
    }

    /// <summary>The next message of the stream.</summary>
    public void Add(string subject, ReadOnlySpan<byte> payload, string? messageId = null, ulong sequence = 0, DateTimeOffset time = default)
    {
        if (_messages++ == 0) (_firstSequence, _firstTime) = (sequence, time == default ? null : time);
        _lastSequence = sequence;
        try
        {
            if (!Envelope.TryParse(payload, out var e, out var error)) throw new JsonException(error);
            Process(payload, in e, messageId, sequence, time);
        }
        catch (JsonException ex)
        {
            _unreadable++;
            if (_unreadableExamples.Count < _options.Examples)
                _unreadableExamples.Add(string.Create(CultureInfo.InvariantCulture, $"{subject} at stream sequence {sequence}: {ex.Message}: {Canon.Excerpt(payload)}"));
        }
    }

    private void Process(ReadOnlySpan<byte> doc, in Envelope e, string? messageId, ulong sequence, DateTimeOffset time)
    {
        if (!e.Server.Present || !e.Db.Present || !e.Schema.Present || !e.Table.Present) throw new JsonException("no source with server, db, schema and table");
        if (e.Op is not ((byte)'c' or (byte)'u' or (byte)'d' or (byte)'r' or (byte)'l')) { _unknown++; return; }
        var table = Table(doc, in e);
        var db = table.Db;
        table.Events++;
        var ofLoad = e.Op is (byte)'r' or (byte)'l';
        // the same message again: delivery is at-least-once, and the server drops repeats only inside its window
        if (ofLoad && messageId is not null && _options.ReplayWindow > 0 && db.Repeated(messageId, _options.ReplayWindow)) { table.Replays++; return; }
        var incarnation = e.Incarnation.Present ? e.Incarnation.Of(doc) : default;
        if (db.Incarnation is null) db.Incarnation = incarnation.ToArray();
        else if (!incarnation.SequenceEqual(db.Incarnation)) NewIncarnation(db, incarnation, sequence);

        if (e.Op == (byte)'l')
        {
            if (e.Phase == Phase.End && db.Pending.Count > 0) Settle(db);
            Marker(table, doc, in e, sequence, time);
            return;
        }
        if (e.Op == (byte)'r')
        {
            // rows of a load come between transactions. They have no commit: they are neither compared with the high-water mark nor move it
            if (db.Pending.Count > 0) Settle(db);
            LoadRow(table, doc, in e, sequence);
            return;
        }
        if (!e.CommitLsn.Present || !Envelope.TryLsn(e.CommitLsn.Of(doc), out var hi, out var slot) || e.Seq < 0) throw new JsonException("a change without commit_lsn and seq");
        if (db.HasPosition && (hi, slot, e.Seq).CompareTo((db.Hi, db.Slot, db.Seq)) <= 0) { table.Replays++; return; }
        if (db.Pending.Count > 0 && (hi != db.PendingHi || slot != db.PendingSlot)) Settle(db);
        (db.PendingHi, db.PendingSlot) = (hi, slot);
        Change(table, doc, in e, sequence);
        (db.HasPosition, db.Hi, db.Slot, db.Seq) = (true, hi, slot, e.Seq);
    }

    private TableState Table(ReadOnlySpan<byte> doc, in Envelope e)
    {
        var length = e.Server.Length + e.Db.Length + e.Schema.Length + e.Table.Length + 3;
        if (_id.Length < length) _id = new byte[length * 2];
        var id = _id.AsSpan(0, length);
        id.Clear(); // a raw JSON string holds no byte 0: it separates
        e.Server.Of(doc).CopyTo(id);
        var dbLength = e.Server.Length + 1 + e.Db.Length;
        e.Db.Of(doc).CopyTo(id[(e.Server.Length + 1)..]);
        e.Schema.Of(doc).CopyTo(id[(dbLength + 1)..]);
        e.Table.Of(doc).CopyTo(id[(dbLength + 2 + e.Schema.Length)..]);
        if (_last is not null && id.SequenceEqual(_last.Id)) return _last;
        if (!_tables.GetAlternateLookup<ReadOnlySpan<byte>>().TryGetValue(id, out var table))
        {
            if (!_dbs.GetAlternateLookup<ReadOnlySpan<byte>>().TryGetValue(id[..dbLength], out var db))
                _dbs.Add(id[..dbLength].ToArray(), db = new DbState(Canon.Name(e.Server.Of(doc)), Canon.Name(e.Db.Of(doc))));
            table = new TableState(db, id.ToArray(), Canon.Name(e.Schema.Of(doc)), Canon.Name(e.Table.Of(doc)));
            _tables.Add(table.Id, table);
            _order.Add(table);
            db.Tables.Add(table);
        }
        return _last = table;
    }

    /// <summary>A restore, or a database of the name created anew: positions start over and the rows are others. Nothing known holds.</summary>
    private void NewIncarnation(DbState db, ReadOnlySpan<byte> incarnation, ulong sequence)
    {
        _notes.Add(string.Create(CultureInfo.InvariantCulture, $"{db.Server}/{db.Name}: incarnation changed from {Canon.Name(db.Incarnation)} to {Canon.Name(incarnation)} at stream sequence {sequence}: the state of its {db.Tables.Count} table(s) was reset; each needs a new historical load to be verified"));
        foreach (var t in db.Tables) t.Reset("the database's incarnation changed");
        db.Pending.Clear();
        db.Incarnation = incarnation.ToArray();
        db.HasPosition = false;
    }

    // ---- changes -------------------------------------------------------------------------------------------------------------------

    private void Change(TableState t, ReadOnlySpan<byte> doc, in Envelope e, ulong sequence)
    {
        t.Changes++;
        var op = e.Op;
        if (op != (byte)'d' && !e.After.Present) throw new JsonException("an insert or update without an after image");
        if (!e.Key.Present || !ReadKey(t, doc, e.Key, out var keyLength)) { t.WithoutKey++; return; }
        Image? before = null, after = null;
        if (op != (byte)'c' && e.Before.Present) (before = _a).Read(doc, e.Before, t, ref _scratch, change: true);
        if (op != (byte)'d') (after = _b).Read(doc, e.After, t, ref _scratch, change: true);

        // the key is that of the after image, or of the before image of a delete. An update may have moved the row from another key
        var ka = _ka.AsSpan(0, keyLength);
        var ha = RowStore.Hash(ka);
        var kb = ka;
        var (hb, moved) = (ha, false);
        if (op == (byte)'u' && before is not null)
        {
            if (!KeyOf(t, doc, before, out var beforeLength)) { t.NotCheckable++; before = null; }
            else if (!_kb.AsSpan(0, beforeLength).SequenceEqual(ka))
            {
                kb = _kb.AsSpan(0, beforeLength);
                (hb, moved) = (RowStore.Hash(kb), true);
            }
        }

        var wasDisplaced = false;
        if (before is not null)
        {
            // CHAIN: the before image is the row as the stream left it
            Displaced? displaced = null;
            t.Displaced?.GetAlternateLookup<ReadOnlySpan<byte>>().TryGetValue(kb, out displaced);
            long address;
            if (displaced is { Known: true } && Differing(t, displaced.Shape, displaced.Cells, before, count: false) == 0)
            {
                // a row another one took the key of earlier in this transaction (1 -> 2 while 2 -> 3): it is this one that leaves now
                t.Displaced!.GetAlternateLookup<ReadOnlySpan<byte>>().Remove(kb);
                wasDisplaced = true;
            }
            else if ((address = t.Rows.Find(kb, hb)) >= 0 && !(displaced is { Known: false } && Differing(t, t.Rows.Shape(address), t.Rows.Cells(address), before, count: false) > 0))
            {
                if (Differing(t, t.Rows.Shape(address), t.Rows.Cells(address), before, count: true) > 0)
                    Report(t, DifferenceKind.BeforeImage, kb, t.Rows.Shape(address), t.Rows.Cells(address), before, doc, in e, sequence, "the before image is not the row the stream had: a change to this row is missing");
                if (op == (byte)'d' || moved) t.Rows.Remove(kb, hb);
            }
            else if (displaced is not null)
            {
                // the row that was displaced, though not as the stream had it; or one the stream never said anything about: nothing to compare
                if (displaced.Known)
                {
                    Differing(t, displaced.Shape, displaced.Cells, before, count: true);
                    Report(t, DifferenceKind.BeforeImage, kb, displaced.Shape, displaced.Cells, before, doc, in e, sequence, "the before image is not the row the stream had: a change to this row is missing");
                }
                t.Displaced!.GetAlternateLookup<ReadOnlySpan<byte>>().Remove(kb);
                wasDisplaced = true;
            }
            else if (t.Complete)
            {
                _differing.Clear();
                Report(t, DifferenceKind.RowNotThere, kb, 0, default, null, doc, in e, sequence, op == (byte)'d' ? "a delete of a row the stream does not have: its insert is missing" : "an update of a row the stream does not have: its insert is missing");
            }
        }
        else if (op == (byte)'d') t.Rows.Remove(kb, hb);

        if (after is null) return;
        if (op == (byte)'c' || moved || wasDisplaced)
        {
            var address = t.Rows.Find(ka, ha);
            if (address >= 0) Displace(t, ka, address, doc, in e, sequence);
            else if (!t.Complete && op == (byte)'u')
            {
                // a row moves onto a key the stream said nothing about yet: there may be a row, whose own change follows in this transaction
                (t.Displaced ??= new Dictionary<byte[], Displaced>(Bytes.Comparer)).TryAdd(ka.ToArray(), new Displaced(-1, [], "", sequence));
                if (!t.Db.Pending.Contains(t)) t.Db.Pending.Add(t);
            }
        }
        t.Rows.Put(ka, ha, after.Shape, t.Epoch, after.Cells.AsSpan(0, after.Count * Canon.Cell));
    }

    /// <summary>
    /// A row arrives on a key that has one. Whether that is a missing delete is known when the transaction ends: on a heap, UPDATE t SET id = 3 - id
    /// changes row 1 to 2 while the other row 2 is still there, and its own change comes next. Until then the row that was there waits.
    /// </summary>
    private void Displace(TableState t, ReadOnlySpan<byte> key, long address, ReadOnlySpan<byte> doc, in Envelope e, ulong sequence)
    {
        t.Displaced ??= new Dictionary<byte[], Displaced>(Bytes.Comparer);
        var waiting = new Displaced(t.Rows.Shape(address), t.Rows.Cells(address).ToArray(), Position(doc, in e), sequence);
        var displaced = t.Displaced.GetAlternateLookup<ReadOnlySpan<byte>>();
        if (displaced.TryGetValue(key, out var first) && first.Known) Count(t, DifferenceKind.RowAlreadyThere, key, waiting.Position, waiting.Sequence); // twice in one transaction
        else displaced[key] = waiting;
        if (!t.Db.Pending.Contains(t)) t.Db.Pending.Add(t);
    }

    /// <summary>The transaction is over: rows that were displaced and never left were deleted without the stream saying so.</summary>
    private void Settle(DbState db)
    {
        foreach (var t in db.Pending)
        {
            if (t.Displaced is null) continue;
            foreach (var d in t.Displaced)
                if (d.Value.Known) Count(t, DifferenceKind.RowAlreadyThere, d.Key, d.Value.Position, d.Value.Sequence);
            t.Displaced = null;
        }
        db.Pending.Clear();
    }

    private void Count(TableState t, DifferenceKind kind, ReadOnlySpan<byte> key, string position, ulong sequence)
    {
        if (!Counted(t, kind)) return;
        Publish(t, new Difference(t.Name, kind, Canon.KeyText(key, t.KeyNames()), [], position, sequence, "an insert or a key change onto a key that is there: the delete of the row that was there is missing"));
    }

    // ---- loads ---------------------------------------------------------------------------------------------------------------------

    private void LoadRow(TableState t, ReadOnlySpan<byte> doc, in Envelope e, ulong sequence)
    {
        t.LoadRows++;
        if (!e.After.Present) throw new JsonException("a row of a load without an after image");
        Load? load = null;
        if (e.Snapshot.Present) t.Loads?.GetAlternateLookup<ReadOnlySpan<byte>>().TryGetValue(e.Snapshot.Of(doc), out load);
        // without a key the load's rows are counted as they come
        if (!e.Key.Present || !ReadKey(t, doc, e.Key, out var keyLength)) { t.WithoutKey++; if (load is not null) load.Rows++; return; }
        _b.Read(doc, e.After, t, ref _scratch, change: false);
        var key = _ka.AsSpan(0, keyLength);
        var hash = RowStore.Hash(key);
        var address = t.Rows.Find(key, hash);
        // a chunk may be published again after a fault (other message ids, perhaps other rows): the load has as many rows as keys were read
        if (load is not null && (address < 0 || t.Rows.Stamp(address) != (t.Epoch | TableState.Read))) load.Rows++;
        if (address >= 0)
        {
            // LOAD: the table's row at this place in the stream is the row the stream has built. True for every row that is known, complete or not
            if (Differing(t, t.Rows.Shape(address), t.Rows.Cells(address), _b, count: true) > 0)
                Report(t, DifferenceKind.LoadRowDiffers, key, t.Rows.Shape(address), t.Rows.Cells(address), _b, doc, in e, sequence, "the table's row is not the row the stream had: a change to this row is missing");
        }
        else if (t.Complete)
        {
            _differing.Clear();
            Report(t, DifferenceKind.LoadRowNotInStream, key, 0, default, null, doc, in e, sequence, "the table has a row the stream does not have: its insert is missing");
        }
        t.Rows.Put(key, hash, _b.Shape, t.Epoch | TableState.Read, _b.Cells.AsSpan(0, _b.Count * Canon.Cell)); // and the state is the table's again
    }

    private void Marker(TableState t, ReadOnlySpan<byte> doc, in Envelope e, ulong sequence, DateTimeOffset time)
    {
        var id = e.LoadId.Present ? e.LoadId : e.Snapshot;
        if (!id.Present || e.Phase == Phase.None) throw new JsonException("a load marker without load.id and load.phase");
        t.Loads ??= new Dictionary<byte[], Load>(Bytes.Comparer);
        var loads = t.Loads.GetAlternateLookup<ReadOnlySpan<byte>>();
        if (e.Phase == Phase.Begin)
        {
            // every row touched from here on carries the new epoch: read by the load, or changed while it ran
            t.Epoch++;
            if (loads.TryGetValue(id.Of(doc), out var again))
            {
                // interrupted and started over: only what follows this begin is the load
                (again.Epoch, again.Rows, again.CompleteAtBegin) = (t.Epoch, 0, t.Complete);
                t.LoadsStartedOver++;
            }
            else loads[id.Of(doc)] = new Load { Epoch = t.Epoch, CompleteAtBegin = t.Complete };
            return;
        }
        if (!loads.Remove(id.Of(doc), out _, out var load)) { t.EndsWithoutBegin++; return; } // begun before the stream begins, or before a reset
        t.LoadsEnded++;
        // the load announces the rows of the chunks that were acknowledged; a chunk published twice makes more messages, never fewer keys
        if (_options.CheckLoadRows && e.Rows > load.Rows)
        {
            if (Counted(t, DifferenceKind.LoadRowCount))
                Publish(t, new Difference(t.Name, DifferenceKind.LoadRowCount, null, [], Position(doc, in e), sequence,
                    string.Create(CultureInfo.InvariantCulture, $"load {Canon.Name(id.Of(doc))} announced {e.Rows} row(s), {load.Rows} arrived since its last begin: rows of the load are missing in the stream; the table was not compared")));
            // which rows the load read is not known then: what did not arrive cannot be told from what is not in the table
            return;
        }
        // LOAD: every row that exists was read or changed between begin and end. What the state has beyond that, the table does not have
        for (var slot = 0; slot < t.Rows.Slots; slot++)
        {
            var address = t.Rows.AddressAt(slot);
            if (address < 0 || (t.Rows.Stamp(address) & ~TableState.Read) >= load.Epoch) continue;
            if (Counted(t, DifferenceKind.RowNotInLoad))
                Publish(t, new Difference(t.Name, DifferenceKind.RowNotInLoad, Canon.KeyText(t.Rows.Key(address), t.KeyNames()), [], Position(doc, in e), sequence,
                    $"the stream has a row that load {Canon.Name(id.Of(doc))} neither read nor saw changed: the table does not have it, its delete is missing"));
            t.Rows.RemoveAt(slot);
        }
        t.Rows.Trim();
        var at = string.Create(CultureInfo.InvariantCulture, $"stream sequence {sequence}{(time == default ? "" : $" ({time.UtcDateTime:yyyy-MM-dd HH:mm:ss}Z)")}, load {Canon.Name(id.Of(doc))}");
        if (!t.Complete) (t.Complete, t.Baseline) = (true, "first complete load at " + at);
        if (load.CompleteAtBegin) (t.Compared, t.ComparedNow, t.LastCompared) = (t.Compared + 1, t.ComparedNow + 1, at);
    }

    // ---- rows ----------------------------------------------------------------------------------------------------------------------

    /// <summary>The key object into <see cref="_ka"/>; false when there is none. Key columns that are others than before: another table.</summary>
    private bool ReadKey(TableState t, ReadOnlySpan<byte> doc, Slice where, out int length)
    {
        length = 0;
        var source = where.Of(doc);
        var r = Canon.Reader(source);
        r.Read();
        var (n, same) = (0, t.KeyColumns is not null);
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            if (n == _keyColumns.Length) Array.Resize(ref _keyColumns, n * 2);
            var name = r.ValueSpan;
            if (same && n < t.KeyColumns!.Length && t.ColumnNames[t.KeyColumns[n]].AsSpan().SequenceEqual(name)) _keyColumns[n] = t.KeyColumns[n];
            else (same, _keyColumns[n]) = (false, t.Column(name));
            n++;
            r.Read();
            Canon.AppendKeyPart(ref r, source, ref _ka, ref length, ref _scratch);
        }
        if (n == 0) return false;
        if (!same || n != t.KeyColumns!.Length)
        {
            if (t.KeyColumns is not null) t.Reset("its key columns changed");
            t.KeyColumns = _keyColumns.AsSpan(0, n).ToArray();
        }
        length = Shorten(ref _ka, length);
        return true;
    }

    /// <summary>The key of an image: its values of the key columns, into <see cref="_kb"/>. False when one is not there or not known.</summary>
    private bool KeyOf(TableState t, ReadOnlySpan<byte> doc, Image image, out int length)
    {
        length = 0;
        foreach (var column in t.KeyColumns!)
        {
            var i = image.Columns.AsSpan(0, image.Count).IndexOf(column);
            if (i < 0 || Canon.KindOf(image.Cells[i * Canon.Cell]) == ValueKind.Unknown) return false;
            var source = doc[image.Starts[i]..image.Ends[i]];
            var r = Canon.Reader(source);
            r.Read();
            Canon.AppendKeyPart(ref r, source, ref _kb, ref length, ref _scratch);
        }
        length = Shorten(ref _kb, length);
        return true;
    }

    private static int Shorten(ref byte[] key, int length)
    {
        if (length <= RowStore.MaxKey) return length;
        var hash = XxHash128.Hash(key.AsSpan(0, length));
        key[0] = 0xFF; // no kind
        hash.CopyTo(key, 1);
        return 1 + hash.Length;
    }

    /// <summary>
    /// Columns in which the row the stream has built and an image of it differ, into <see cref="_differing"/>. Only columns both have are compared
    /// (the others were added or dropped), a value either side does not know matches, and a value of another JSON kind is a retyped column.
    /// </summary>
    private int Differing(TableState t, int shape, ReadOnlySpan<byte> cells, Image image, bool count)
    {
        _differing.Clear();
        var retyped = 0;
        if (shape == image.Shape)
        {
            for (var i = 0; i < image.Count; i++)
                if (Differs(cells.Slice(i * Canon.Cell, Canon.Cell), image.Cells.AsSpan(i * Canon.Cell, Canon.Cell), ref retyped)) _differing.Add((i, i));
        }
        else
        {
            if (_map.Length < t.ColumnNames.Count) _map = new int[t.ColumnNames.Count * 2];
            var map = _map.AsSpan(0, t.ColumnNames.Count);
            map.Fill(-1);
            for (var i = 0; i < image.Count; i++) map[image.Columns[i]] = i;
            var columns = t.Shapes[shape];
            for (var j = 0; j < columns.Length; j++)
                if (map[columns[j]] is >= 0 and var i && Differs(cells.Slice(j * Canon.Cell, Canon.Cell), image.Cells.AsSpan(i * Canon.Cell, Canon.Cell), ref retyped)) _differing.Add((j, i));
        }
        if (count) t.Retyped += retyped;
        return _differing.Count;
    }

    private static bool Differs(ReadOnlySpan<byte> expected, ReadOnlySpan<byte> got, ref int retyped)
    {
        if (expected.SequenceEqual(got)) return false;
        var (a, b) = (Canon.ClassOf(expected[0]), Canon.ClassOf(got[0]));
        if (a == (int)ValueKind.Unknown || b == (int)ValueKind.Unknown) return false;
        // number <-> string and the like: the column's type changed, which rewrites every row without an event. NULL is a value of every type
        if (a != b && a != (int)ValueKind.Null && b != (int)ValueKind.Null) { retyped++; return false; }
        return true;
    }

    // ---- reporting -----------------------------------------------------------------------------------------------------------------

    private bool Counted(TableState t, DifferenceKind kind)
    {
        Differences++;
        return ++t.ByKind[(int)kind] <= _options.Examples || OnDifference is not null;
    }

    private void Publish(TableState t, Difference d)
    {
        if (t.ByKind[(int)d.Kind] <= _options.Examples) t.Examples.Add(d);
        OnDifference?.Invoke(d);
    }

    private void Report(TableState t, DifferenceKind kind, ReadOnlySpan<byte> key, int shape, ReadOnlySpan<byte> cells, Image? image, ReadOnlySpan<byte> doc, in Envelope e, ulong sequence, string what)
    {
        if (!Counted(t, kind)) return;
        var columns = new List<ColumnDifference>();
        foreach (var (state, got) in _differing)
        {
            if (columns.Count == 8) { columns.Add(new ColumnDifference("...", "", $"{_differing.Count - 8} more column(s)")); break; }
            columns.Add(new ColumnDifference(Canon.Name(t.ColumnNames[t.Shapes[shape][state]]), Canon.CellText(cells.Slice(state * Canon.Cell, Canon.Cell)), Canon.Excerpt(doc[image!.Starts[got]..image.Ends[got]])));
        }
        Publish(t, new Difference(t.Name, kind, Canon.KeyText(key, t.KeyNames()), columns, Position(doc, in e), sequence, what));
    }

    private static string Position(ReadOnlySpan<byte> doc, in Envelope e)
    {
        var lsn = e.Lsn.Present ? Encoding.UTF8.GetString(e.Lsn.Of(doc)) : "?";
        return e.CommitLsn.Present && e.Seq >= 0 ? string.Create(CultureInfo.InvariantCulture, $"commit {Encoding.UTF8.GetString(e.CommitLsn.Of(doc))} seq {e.Seq}") : "lsn " + lsn;
    }

    /// <summary>Where every table stands.</summary>
    public VerificationReport Report() => new()
    {
        Messages = _messages, FirstSequence = _firstSequence, LastSequence = _lastSequence, FirstTime = _firstTime, Unreadable = _unreadable,
        UnreadableExamples = [.. _unreadableExamples], UnknownOperations = _unknown, Notes = [.. _notes], Tables = [.. _order.Select(TableReport)],
    };

    private static TableReport TableReport(TableState t)
    {
        var differences = t.ByKind.Sum();
        var keyless = t.KeyColumns is null && t.WithoutKey > 0;
        var (verdict, reason) = differences > 0 ? (Verdict.Different, $"{differences} difference(s) between the stream and the table; after each the state took what the stream says")
            : keyless ? (Verdict.CannotBeVerified, $"its {t.WithoutKey} event(s) have no key: rows cannot be told apart")
            : t.ComparedNow > 0 ? (Verdict.Verified, $"{t.ComparedNow} historical load(s) compared row by row with the state the stream had built, the last at {t.LastCompared}; every change is chained to it")
            : t.Complete ? (Verdict.NotVerifiedYet, "one complete load is the baseline; the next load will be compared with it (changes since are checked against it)")
            : t.Loads is { Count: > 0 } ? (Verdict.NotVerifiedYet, "a historical load has begun and not ended")
            : t.Resets > 0 ? (Verdict.NotVerifiedYet, $"its state was reset ({t.ResetReason}) and no complete load came since")
            : (Verdict.NotVerifiedYet, "the stream does not reach back to a complete historical load of this table: only changes to rows seen before are checked");
        return new TableReport
        {
            Server = t.Db.Server, Database = t.Db.Name, Schema = t.Schema, Table = t.Table, Incarnation = t.Db.Incarnation is null ? "" : Canon.Name(t.Db.Incarnation),
            Verdict = verdict, Reason = reason, Baseline = t.Baseline, RowsInState = t.Rows.Count, StateBytes = t.Rows.Bytes, Events = t.Events, Changes = t.Changes,
            LoadRows = t.LoadRows, Loads = t.LoadsEnded, LoadsCompared = t.Compared, LoadsStartedOver = t.LoadsStartedOver, ReplaysSkipped = t.Replays, Retyped = t.Retyped,
            WithoutKey = t.WithoutKey, NotCheckable = t.NotCheckable, Undecided = t.Displaced?.Count(d => d.Value.Known) ?? 0, Resets = t.Resets, Differences = differences,
            ByKind = Enum.GetValues<DifferenceKind>().ToDictionary(k => k, k => t.ByKind[(int)k]), Examples = [.. t.Examples],
        };
    }

    /// <summary>The rows a table's state holds, each as its columns' cells: for tests that compare it with the table.</summary>
    internal Dictionary<string, string> Dump(string server, string database, string schema, string table)
    {
        var rows = new Dictionary<string, string>(StringComparer.Ordinal);
        var t = _order.FirstOrDefault(t => t.Db.Server == server && t.Db.Name == database && t.Schema == schema && t.Table == table);
        if (t is null) return rows;
        for (var slot = 0; slot < t.Rows.Slots; slot++)
        {
            var address = t.Rows.AddressAt(slot);
            if (address < 0) continue;
            var columns = t.Shapes[t.Rows.Shape(address)];
            var cells = t.Rows.Cells(address).ToArray();
            rows.Add(Canon.KeyText(t.Rows.Key(address), t.KeyNames()), string.Join(";", columns.Select((c, i) => $"{Canon.Name(t.ColumnNames[c])}={Canon.CellText(cells.AsSpan(i * Canon.Cell, Canon.Cell))}").Order(StringComparer.Ordinal)));
        }
        return rows;
    }

    /// <summary>What <see cref="Dump"/> shows for a key object.</summary>
    internal static string KeyText(ReadOnlySpan<byte> json)
    {
        var r = Canon.Reader(json);
        r.Read();
        var (key, length, scratch, names) = (new byte[64], 0, new byte[json.Length], new List<string>());
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            names.Add(Canon.Name(r.ValueSpan));
            r.Read();
            Canon.AppendKeyPart(ref r, json, ref key, ref length, ref scratch);
        }
        return Canon.KeyText(key.AsSpan(0, length), names);
    }

    /// <summary>What <see cref="Dump"/> shows for a value.</summary>
    internal static string CellText(ReadOnlySpan<byte> json)
    {
        var r = Canon.Reader(json);
        r.Read();
        Span<byte> cell = stackalloc byte[Canon.Cell];
        var scratch = new byte[json.Length];
        Canon.WriteCell(ref r, json, cell, ref scratch);
        return Canon.CellText(cell);
    }
}

internal enum Phase : byte { None, Begin, End }

/// <summary>A part of the message, by position: nothing is copied out of it.</summary>
internal readonly record struct Slice(int Start, int Length)
{
    public static readonly Slice None = new(0, -1);
    public bool Present => Length >= 0;
    public ReadOnlySpan<byte> Of(ReadOnlySpan<byte> doc) => doc.Slice(Start, Length);
}

/// <summary>The envelope of one message: where its parts are. Strings are the raw text between their quotes.</summary>
internal struct Envelope
{
    public byte Op;
    public Phase Phase;
    public Slice Key, Before, After, Server, Db, Incarnation, Schema, Table, Lsn, CommitLsn, Snapshot, LoadId;
    /// <summary>-1 when the message has none (rows of a load, markers).</summary>
    public long Seq, Rows;

    public static bool TryParse(ReadOnlySpan<byte> doc, out Envelope e, out string? error)
    {
        e = new Envelope { Key = Slice.None, Before = Slice.None, After = Slice.None, Server = Slice.None, Db = Slice.None, Incarnation = Slice.None, Schema = Slice.None, Table = Slice.None, Lsn = Slice.None, CommitLsn = Slice.None, Snapshot = Slice.None, LoadId = Slice.None, Seq = -1, Rows = -1 };
        error = null;
        var r = Canon.Reader(doc);
        if (!r.Read() || r.TokenType != JsonTokenType.StartObject) { error = "not a JSON object"; return false; }
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            if (r.ValueTextEquals("op"u8))
            {
                r.Read();
                e.Op = r.TokenType == JsonTokenType.String && r.ValueSpan.Length == 1 ? r.ValueSpan[0] : (byte)'?';
                r.Skip();
            }
            else if (r.ValueTextEquals("key"u8)) e.Key = Object(ref r);
            else if (r.ValueTextEquals("before"u8)) e.Before = Object(ref r);
            else if (r.ValueTextEquals("after"u8)) e.After = Object(ref r);
            else if (r.ValueTextEquals("source"u8))
            {
                if (!r.Read() || r.TokenType != JsonTokenType.StartObject) { error = "source is not an object"; return false; }
                while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
                {
                    if (r.ValueTextEquals("server"u8)) e.Server = String(ref r);
                    else if (r.ValueTextEquals("db"u8)) e.Db = String(ref r);
                    else if (r.ValueTextEquals("incarnation"u8)) e.Incarnation = String(ref r);
                    else if (r.ValueTextEquals("schema"u8)) e.Schema = String(ref r);
                    else if (r.ValueTextEquals("table"u8)) e.Table = String(ref r);
                    else if (r.ValueTextEquals("lsn"u8)) e.Lsn = String(ref r);
                    else if (r.ValueTextEquals("commit_lsn"u8)) e.CommitLsn = String(ref r);
                    else if (r.ValueTextEquals("snapshot"u8)) e.Snapshot = String(ref r);
                    else if (r.ValueTextEquals("seq"u8)) e.Seq = Number(ref r);
                    else { r.Read(); r.Skip(); }
                }
            }
            else if (r.ValueTextEquals("load"u8))
            {
                if (!r.Read() || r.TokenType != JsonTokenType.StartObject) { r.Skip(); continue; }
                while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
                {
                    if (r.ValueTextEquals("id"u8)) e.LoadId = String(ref r);
                    else if (r.ValueTextEquals("rows"u8)) e.Rows = Number(ref r);
                    else if (r.ValueTextEquals("phase"u8))
                    {
                        r.Read();
                        e.Phase = r.TokenType != JsonTokenType.String ? Phase.None : r.ValueTextEquals("begin"u8) ? Phase.Begin : r.ValueTextEquals("end"u8) ? Phase.End : Phase.None;
                        r.Skip();
                    }
                    else { r.Read(); r.Skip(); }
                }
            }
            else { r.Read(); r.Skip(); }
        }
        if (r.Read()) { error = "something follows the object"; return false; }
        if (e.Op == 0) { error = "no op"; return false; }
        return true;
    }

    private static Slice Object(ref Utf8JsonReader r)
    {
        r.Read();
        if (r.TokenType == JsonTokenType.Null) return Slice.None;
        if (r.TokenType != JsonTokenType.StartObject) throw new JsonException("key, before and after are objects or null");
        var start = (int)r.TokenStartIndex;
        r.Skip();
        return new Slice(start, (int)r.BytesConsumed - start);
    }

    private static Slice String(ref Utf8JsonReader r)
    {
        r.Read();
        if (r.TokenType != JsonTokenType.String) { r.Skip(); return Slice.None; }
        return new Slice((int)r.TokenStartIndex + 1, r.ValueSpan.Length);
    }

    private static long Number(ref Utf8JsonReader r)
    {
        r.Read();
        if (r.TokenType == JsonTokenType.Number && r.TryGetInt64(out var n) && n >= 0) return n;
        r.Skip();
        return -1;
    }

    /// <summary>"VVVVVVVV:BBBBBBBB:SSSS" in hex; its order is that of (V, B, S).</summary>
    public static bool TryLsn(ReadOnlySpan<byte> s, out ulong high, out ushort slot)
    {
        (high, slot) = (0, 0);
        if (s.Length == 24 && s[0] == (byte)'0' && (s[1] | 0x20) == (byte)'x') s = s[2..];
        if (s.Length != 22 || s[8] != (byte)':' || s[17] != (byte)':') return false;
        if (!System.Buffers.Text.Utf8Parser.TryParse(s[..8], out uint vlf, out var n, 'X') || n != 8) return false;
        if (!System.Buffers.Text.Utf8Parser.TryParse(s[9..17], out uint block, out n, 'X') || n != 8) return false;
        if (!System.Buffers.Text.Utf8Parser.TryParse(s[18..], out slot, out n, 'X') || n != 4) return false;
        high = (ulong)vlf << 32 | block;
        return true;
    }
}

/// <summary>One image of a row, read into cells. Reused from message to message.</summary>
internal sealed class Image
{
    public int Count, Shape;
    public int[] Columns = new int[16], Starts = new int[16], Ends = new int[16];
    public byte[] Cells = new byte[16 * Canon.Cell];

    public void Read(ReadOnlySpan<byte> doc, Slice where, TableState t, ref byte[] scratch, bool change)
    {
        var source = where.Of(doc);
        var r = Canon.Reader(source);
        r.Read();
        var expected = t.Shapes.Count > 0 ? t.Shapes[t.LastShape] : [];
        Count = 0;
        while (r.Read() && r.TokenType == JsonTokenType.PropertyName)
        {
            if (Count == Columns.Length)
            {
                if (Count >= ushort.MaxValue) throw new JsonException("an image of more than 65,535 columns");
                Array.Resize(ref Columns, Count * 2);
                Array.Resize(ref Starts, Count * 2);
                Array.Resize(ref Ends, Count * 2);
                Array.Resize(ref Cells, Count * 2 * Canon.Cell);
            }
            var name = r.ValueSpan;
            // the columns of the image before this one, mostly: no lookup
            Columns[Count] = Count < expected.Length && t.ColumnNames[expected[Count]].AsSpan().SequenceEqual(name) ? expected[Count] : t.Column(name);
            r.Read();
            Starts[Count] = where.Start + (int)r.TokenStartIndex;
            Canon.WriteCell(ref r, source, Cells.AsSpan(Count * Canon.Cell, Canon.Cell), ref scratch);
            Ends[Count] = where.Start + (int)r.BytesConsumed;
            Count++;
        }
        Shape = t.ShapeOf(Columns.AsSpan(0, Count), change);
    }
}

/// <summary>A row that was on a key when another arrived there, until its transaction ends.</summary>
internal sealed record Displaced(int Shape, byte[] Cells, string Position, ulong Sequence)
{
    /// <summary>False: a row that may have been on a key the stream had said nothing about (a state that is not complete).</summary>
    public bool Known => Shape >= 0;
}

internal sealed class Load
{
    /// <summary>The table's epoch at the last begin: what carries an older stamp was neither read nor changed since.</summary>
    public uint Epoch;
    /// <summary>Keys read since the last begin (rows, in a table without a key).</summary>
    public long Rows;
    public bool CompleteAtBegin;
}

internal sealed class DbState(string server, string name)
{
    public readonly string Server = server, Name = name;
    public readonly List<TableState> Tables = [];
    /// <summary>Tables with displaced rows in the transaction at (<see cref="PendingHi"/>, <see cref="PendingSlot"/>).</summary>
    public readonly List<TableState> Pending = [];
    public byte[]? Incarnation;
    /// <summary>The high-water mark: the last change applied.</summary>
    public bool HasPosition;
    public ulong Hi, PendingHi;
    public ushort Slot, PendingSlot;
    public long Seq;
    private HashSet<ulong>? _ids;
    private Queue<ulong>? _order;

    /// <summary>Has this message id been seen among the last ones? Remembers it.</summary>
    public bool Repeated(string id, int window)
    {
        var hash = XxHash3.HashToUInt64(MemoryMarshal.AsBytes(id.AsSpan()));
        _ids ??= [];
        _order ??= new Queue<ulong>();
        if (!_ids.Add(hash)) return true;
        _order.Enqueue(hash);
        if (_order.Count > window) _ids.Remove(_order.Dequeue());
        return false;
    }
}

internal sealed class TableState(DbState db, byte[] id, string schema, string table)
{
    public readonly DbState Db = db;
    public readonly byte[] Id = id;
    public readonly string Schema = schema, Table = table;
    public readonly RowStore Rows = new();
    /// <summary>Column names as they stand in the messages (raw), by number; a row's shape is the numbers of its columns in order.</summary>
    public readonly List<byte[]> ColumnNames = [];
    public readonly List<int[]> Shapes = [];
    public readonly long[] ByKind = new long[Enum.GetValues<DifferenceKind>().Length];
    public readonly List<Difference> Examples = [];
    private readonly Dictionary<byte[], int> _columns = new(Bytes.Comparer);
    private readonly Dictionary<ulong, int> _shapes = [];
    private string[]? _keyNames;
    public int LastShape;
    public int[]? KeyColumns;
    /// <summary>Stamped on every row that is read by a load or changed; one more with every begin of a load.</summary>
    public uint Epoch = 1;
    /// <summary>In a row's stamp: it was read by a load in its epoch (and not changed since), so reading it again is not another row.</summary>
    public const uint Read = 1u << 31;
    /// <summary>A load has ended since the state was empty: a key that is not in the state is not in the table.</summary>
    public bool Complete;
    public Dictionary<byte[], Load>? Loads;
    public Dictionary<byte[], Displaced>? Displaced;
    public string? Baseline, LastCompared, ResetReason;
    public long Events, Changes, LoadRows, Replays, Retyped, WithoutKey, NotCheckable, LoadsEnded, LoadsStartedOver, Compared, ComparedNow, EndsWithoutBegin, Resets;

    public string Name => $"{Db.Server}/{Db.Name}/{Schema}.{Table}";

    public int Column(ReadOnlySpan<byte> name)
    {
        var lookup = _columns.GetAlternateLookup<ReadOnlySpan<byte>>();
        if (lookup.TryGetValue(name, out var n)) return n;
        var copy = name.ToArray();
        ColumnNames.Add(copy);
        _columns.Add(copy, ColumnNames.Count - 1);
        return ColumnNames.Count - 1;
    }

    public int ShapeOf(ReadOnlySpan<int> columns, bool change)
    {
        var shape = Find(columns);
        // A column that changes had and have no more was dropped: if its name comes back, that is another column, with other values. Only changes
        // tell: they come in the order of the log, while a row of a load may have been read before a column was added and be published after.
        if (change && shape != _lastChange)
        {
            if (_lastChange >= 0)
                foreach (var c in Shapes[_lastChange])
                    if (!columns.Contains(c) && _columns.TryGetValue(ColumnNames[c], out var current) && current == c) _columns.Remove(ColumnNames[c]);
            _lastChange = shape;
        }
        return shape;
    }

    private int _lastChange = -1;

    private int Find(ReadOnlySpan<int> columns)
    {
        if (Shapes.Count > 0 && columns.SequenceEqual(Shapes[LastShape])) return LastShape;
        for (var hash = XxHash3.HashToUInt64(MemoryMarshal.AsBytes(columns)); ; hash++)
        {
            if (!_shapes.TryGetValue(hash, out var n))
            {
                Shapes.Add(columns.ToArray());
                _shapes.Add(hash, n = Shapes.Count - 1);
            }
            else if (!columns.SequenceEqual(Shapes[n])) continue;
            return LastShape = n;
        }
    }

    public string[] KeyNames() => KeyColumns is null ? [] : _keyNames is { } names && names.Length == KeyColumns.Length ? names : _keyNames = [.. KeyColumns.Select(c => Canon.Name(ColumnNames[c]))];

    /// <summary>Nothing known holds any more. Differences found stay found.</summary>
    public void Reset(string why)
    {
        Rows.Clear();
        (Loads, Displaced, Complete, Baseline, ComparedNow, ResetReason, _keyNames) = (null, null, false, null, 0, why, null);
        Resets++;
    }
}
