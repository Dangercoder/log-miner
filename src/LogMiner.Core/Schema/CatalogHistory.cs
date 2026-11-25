using System.Buffers.Binary;
using System.Collections.Immutable;
using System.Globalization;
using LogMiner.Log;
using LogMiner.Rows;

namespace LogMiner.Schema;

/// <summary>System base tables whose row changes describe the physical layout of user tables.</summary>
public enum CatalogTable : byte
{
    /// <summary>sys.sysrscols: the physical columns of every rowset.</summary>
    RowsetColumns,
    /// <summary>sys.sysrowsets: rowset (partition) to object and index, compression.</summary>
    Rowsets,
    /// <summary>sys.syscolpars: column names.</summary>
    ObjectColumns,
    /// <summary>sys.sysschobjs: object names.</summary>
    Objects,
    /// <summary>sys.sysseobjvalues: default values of columns added without rewriting the rows.</summary>
    Defaults,
}

/// <summary>A row change of a system base table, as found in the log.</summary>
public sealed class CatalogChange
{
    public required Lsn Lsn { get; init; }
    public required ulong TransactionId { get; init; }
    public required LogOperation Operation { get; init; }
    public bool Compensation { get; init; }
    /// <summary>Compensation: everything the transaction logged above this is undone.</summary>
    public Lsn UndoneAfter { get; init; }
    public required CatalogTable Table { get; init; }
    /// <summary>Insert/delete: the row. Modify: the replaced bytes.</summary>
    public byte[]? Row0 { get; init; }
    /// <summary>Modify: the new bytes.</summary>
    public byte[]? Row1 { get; init; }
    /// <summary>Modify: the clustered key record.</summary>
    public byte[]? Key { get; init; }
    /// <summary>Modify: offset of the bytes in the row.</summary>
    public short Offset { get; init; }
    /// <summary>Change of several places: their bytes (<see cref="Row0"/> has the offsets, <see cref="Row1"/> the lengths).</summary>
    public byte[]? Data { get; init; }
}

/// <summary>How a transaction that changed the catalog ended. System transactions (page splits moving rows) are not changes.</summary>
public readonly record struct TransactionOutcome(bool IsSystem, bool Aborted, Lsn Commit)
{
    public bool IsCommitted => !Aborted && !Commit.IsZero;
    public bool IsOpen => !Aborted && Commit.IsZero;
}

/// <summary>A rowset (partition) of the current catalog.</summary>
public sealed record CurrentRowset(long RowsetId, int ObjectId, int IndexId, DataCompression Compression, IReadOnlyList<PhysicalColumn> Columns,
                                   IReadOnlyDictionary<int, object?> Defaults);

/// <summary>Names and key of a table. Columns by column id; key ordinals by column id.</summary>
public sealed record TableIdentity(int ObjectId, string Schema, string Name, IReadOnlyDictionary<int, string> ColumnNames, IReadOnlyDictionary<int, int> KeyOrdinals);

/// <summary>The committed catalog at load time, restricted to the tables of interest.</summary>
public sealed class CatalogSnapshot
{
    public required int DatabaseId { get; init; }
    public required string Database { get; init; }
    public required IReadOnlyDictionary<long, CurrentRowset> Rowsets { get; init; }
    public required IReadOnlyDictionary<int, TableIdentity> Objects { get; init; }
    public required IReadOnlyDictionary<int, string> SchemaNames { get; init; }
    /// <summary>Code page of a collation id.</summary>
    public required Func<int, int> CodePage { get; init; }
}

/// <summary>
/// Rebuilds the layouts captured tables had earlier in the log by undoing the catalog's own logged row changes, newest first, starting
/// from the current catalog. Everything needed is in the log that is retained anyway, so no schema history has to be persisted, and any
/// number of DDL statements since the checkpoint is handled at the exact LSN of each change.
/// <para>Nothing is assumed: every logged row that should still be there must equal the live catalog, every removed row must be absent.
/// A contradiction throws <see cref="CatalogHistoryException"/>.</para>
/// </summary>
public static class CatalogHistory
{
    private sealed record RowsetRow(int ObjectId, int IndexId, DataCompression Compression);

    private sealed record State(RowsetRow? Row, ImmutableDictionary<int, PhysicalColumn> Columns, ImmutableDictionary<int, string> Names);

    private sealed record Change(Lsn Lsn, LogOperation Operation, CatalogTable Table, RowImage? Image, CatalogChange Raw, long Rowset, int ObjectId, int ColumnId);

    private sealed record Context(CatalogSnapshot Now, IReadOnlyDictionary<CatalogTable, TableLayout> Formats, Dictionary<(long Rowset, int HbcolId), object?> Defaults,
                                  Dictionary<int, (string Name, int SchemaId)> DroppedTables);

    public static LayoutHistory Build(CatalogSnapshot now, IReadOnlyList<CatalogChange> changes, IReadOnlyDictionary<ulong, TransactionOutcome> outcomes,
                                      IReadOnlyDictionary<CatalogTable, TableLayout> formats, Lsn coversFrom)
    {
        if (changes.Count == 0) return new LayoutHistory(coversFrom, new Dictionary<long, IReadOnlyList<LayoutVersion>>());
        changes = changes.SelectMany(c => Expand(c, formats)).ToList();
        RefuseRacingChanges(now, changes, outcomes, formats);
        var effective = Effective(changes, outcomes).Select(c => Decode(c, formats)).ToList();

        // which rowset belongs to which table (index 0/1 = the data): today's catalog, plus rowsets created or dropped in the log
        var owner = now.Rowsets.Values.ToDictionary(r => r.RowsetId, r => (r.ObjectId, r.IndexId));
        var dropped = new Dictionary<int, (string Name, int SchemaId)>();
        var defaults = new Dictionary<(long, int), object?>();
        foreach (var c in effective)
        {
            if (c.Image is not { } row) continue;
            if (c.Table == CatalogTable.Rowsets) owner.TryAdd(c.Rowset, (Int(row, "idmajor"), Int(row, "idminor")));
            if (c.Table == CatalogTable.Objects && c.Operation == LogOperation.DeleteRows) dropped[c.ObjectId] = ((string)row["name"]!, Int(row, "nsid"));
            if (c.Table == CatalogTable.Defaults && Int(row, "valclass") == 1 && Int(row, "valnum") == 0)
                defaults[(c.Rowset, (int)Long(row, "subid"))] = DecodeDefault(c.Raw, formats[CatalogTable.Defaults], now.CodePage);
        }
        var context = new Context(now, formats, defaults, dropped);

        var byRowset = effective.Where(c => c.Table is CatalogTable.RowsetColumns or CatalogTable.Rowsets).ToLookup(c => c.Rowset);
        var namesByObject = effective.Where(c => c.Table == CatalogTable.ObjectColumns && c.Image is not null).ToLookup(c => c.ObjectId);
        var history = new Dictionary<long, IReadOnlyList<LayoutVersion>>();
        foreach (var (rowset, (objectId, indexId)) in owner)
        {
            if (indexId is not (0 or 1)) continue;
            if (!now.Objects.ContainsKey(objectId) && !dropped.ContainsKey(objectId)) continue; // not a table we are asked about
            var relevant = byRowset[rowset].Concat(namesByObject[objectId]).OrderBy(c => c.Lsn).ToList();
            if (relevant.Count == 0) continue;
            try { history[rowset] = Undo(context, rowset, objectId, relevant); }
            catch (CatalogHistoryException ex) when (!now.Objects.ContainsKey(objectId))
            {
                // a table that is gone and may never have been captured: only whoever needs its layout has to know
                history[rowset] = [new LayoutVersion(default, () => throw new CatalogHistoryException(ex.Message, ex))];
            }
        }
        return new LayoutHistory(coversFrom, history, Moves(effective, formats[CatalogTable.Rowsets]), Renames(now, effective, formats[CatalogTable.Objects], dropped, owner.Values.Where(o => o.IndexId is 0 or 1).Select(o => o.ObjectId).ToHashSet()));
    }

    /// <summary>
    /// Renames and schema transfers are changes in place to the table's row in sysschobjs, keyed by object id (experiments/38): of the schema's id, or
    /// of the name, which is the row's only variable-length column (its end offset and its bytes, in one or in several places). Undone from today's name
    /// backwards; a table that is gone, from the name it was dropped with.
    /// </summary>
    private static List<TableRename> Renames(CatalogSnapshot now, List<Change> effective, TableLayout format, Dictionary<int, (string Name, int SchemaId)> dropped, HashSet<int> tables)
    {
        var renames = new List<TableRename>();
        var schemaAt = format.Visible.First(c => c.Name == "nsid").LeafOffset;
        var nameAt = NameAt(format);
        if (effective.FirstOrDefault(c => c.Table == CatalogTable.Objects && c.Operation == LogOperation.ModifyColumns
                && (c.Raw.Key is not { Length: >= 5 } k || BinaryPrimitives.ReadInt32LittleEndian(k.AsSpan(1)) is var o && (now.Objects.ContainsKey(o) || tables.Contains(o)))) is { } unknown)
            throw new CatalogHistoryException($"{unknown.Table} {unknown.Operation} at {unknown.Lsn} is not understood: {Convert.ToHexString(unknown.Raw.Row0 ?? [])} {Convert.ToHexString(unknown.Raw.Row1 ?? [])} {Convert.ToHexString(unknown.Raw.Data ?? [])}");
        foreach (var changes in effective.Where(c => c.Table == CatalogTable.Objects && c.Operation == LogOperation.ModifyRow && c.Raw.Key is { Length: >= 5 })
                     .GroupBy(c => BinaryPrimitives.ReadInt32LittleEndian(c.Raw.Key.AsSpan(1))))
        {
            string last;
            int schema;
            if (now.Objects.TryGetValue(changes.Key, out var table))
                (last, schema) = (table.Name, now.SchemaNames.Where(s => s.Value == table.Schema).Select(s => (int?)s.Key).FirstOrDefault() ?? -1);
            else if (dropped.TryGetValue(changes.Key, out var gone)) (last, schema) = gone;
            else continue;
            var nsid = BitConverter.GetBytes(schema);
            var name = new List<byte>();
            name.AddRange(BitConverter.GetBytes((ushort)(nameAt + 2 + 2 * last.Length)));
            name.AddRange(System.Runtime.InteropServices.MemoryMarshal.AsBytes(last.AsSpan())); // exactly, whatever is in it
            try { Undo(changes.Key, changes); }
            catch (CatalogHistoryException) when (!now.Objects.ContainsKey(changes.Key) && !tables.Contains(changes.Key)) { } // a constraint or the like that is gone

            void Undo(int key, IEnumerable<Change> all)
            {
                foreach (var record in all.OrderByDescending(c => c.Lsn).GroupBy(c => c.Lsn)) // the places of one record, the last one first
                {
                    var changed = false;
                    foreach (var c in record)
                    {
                        var (at, before, after) = ((int)c.Raw.Offset, c.Raw.Row0 ?? [], c.Raw.Row1 ?? []);
                        for (var i = 0; i < before.Length && at + i < schemaAt + 4; i++)
                            if (at + i >= schemaAt && before.Length == after.Length) (nsid[at + i - schemaAt], changed) = (before[i], changed || before[i] != after[i]);
                        if (at < nameAt)
                        {
                            var cut = nameAt - at; // a place that reaches from the fixed part into the name
                            if (before.Length <= cut && after.Length <= cut) continue;
                            if (before.Length < cut || after.Length < cut) throw new CatalogHistoryException($"object {key}: the change at {c.Lsn} is not understood");
                            (at, before, after) = (nameAt, before[cut..], after[cut..]);
                        }
                        if (at - nameAt + after.Length > name.Count || !name.Skip(at - nameAt).Take(after.Length).SequenceEqual(after))
                            throw new CatalogHistoryException($"object {key}: the name {last} does not agree with the change at {c.Lsn}");
                        name.RemoveRange(at - nameAt, after.Length);
                        name.InsertRange(at - nameAt, before);
                        changed = true;
                    }
                    if (!changed) continue;
                    var id = BitConverter.ToInt32(nsid);
                    var bytes = name.ToArray();
                    if (bytes.Length < 4 || BitConverter.ToUInt16(bytes) != nameAt + bytes.Length) throw new CatalogHistoryException($"object {key}: the name before the change at {record.Key} is not understood");
                    // a schema that is gone has no name any more (and its id may be another's by now): that it was another one is reported either way
                    renames.Add(new TableRename(record.Key, key, now.SchemaNames.GetValueOrDefault(id) ?? (id == -1 ? table!.Schema : string.Create(CultureInfo.InvariantCulture, $"schema#{id}")),
                        new string(System.Runtime.InteropServices.MemoryMarshal.Cast<byte, char>(bytes.AsSpan(2)))));
                }
            }
        }
        renames.Sort((a, b) => a.At.CompareTo(b.At));
        return renames;
    }

    /// <summary>Where the end offset of the name is in a row of sysschobjs: behind the null bitmap and the number of variable columns.</summary>
    private static int NameAt(TableLayout format) =>
        format.Columns.Where(c => c.LeafOffset >= 4).Max(c => c.LeafOffset + c.MaxInRowLength) + 2 + (format.Columns.Count + 7) / 8 + 2;

    /// <summary>
    /// The same for a row of sysschobjs, where places of the variable part count: the name. Only the length a place had is logged; the one it got follows
    /// from where the next place moved to, and that of the last one from how the name's end offset changed (experiments/38). The last place comes first,
    /// and each where it is after the change: undone in this order, none moves another. What is not understood stays as it is, and matters when it is of a
    /// table that is asked about.
    /// </summary>
    private static List<CatalogChange> ExpandObject(CatalogChange c, TableLayout format)
    {
        var data = c.Data ?? [];
        if (c.Row0 is not { Length: > 0 } offsets || c.Row1 is not { } lengths || offsets.Length % 4 != 0 || lengths.Length * 2 != offsets.Length) return [c];
        var n = lengths.Length / 2;
        int From(int i) => BinaryPrimitives.ReadInt16LittleEndian(offsets.AsSpan(4 * i));
        int To(int i) => BinaryPrimitives.ReadInt16LittleEndian(offsets.AsSpan(4 * i + 2));
        int Had(int i) => BinaryPrimitives.ReadInt16LittleEndian(lengths.AsSpan(2 * i));
        var nameAt = NameAt(format);
        var changes = new List<CatalogChange>();
        var (at, grown, ends) = (0, 0, 0); // bytes before this place became more by; the name's end offset by
        for (var i = 0; i < n; i++)
        {
            var had = Had(i);
            var afterAt = at + ((had + 3) & ~3);
            if (From(i) < 4 || had < 0 || afterAt > data.Length || To(i) != From(i) + grown) return [c];
            for (var b = 0; b < 2; b++) // the end offset lies before the name, where nothing has moved yet
                if (From(i) <= nameAt + b && nameAt + b < From(i) + had)
                {
                    if (afterAt + nameAt + b - From(i) >= data.Length) return [c];
                    ends += (data[afterAt + nameAt + b - From(i)] - data[at + nameAt + b - From(i)]) << (8 * b);
                }
            var has = i < n - 1 ? To(i + 1) - To(i) - From(i + 1) + From(i) + had : had + ends - grown;
            if (has < 0 || afterAt + has > data.Length) return [c];
            changes.Insert(0, new CatalogChange
            {
                Lsn = c.Lsn, Operation = LogOperation.ModifyRow, TransactionId = c.TransactionId, Table = c.Table, Offset = (short)To(i), Key = c.Key, UndoneAfter = c.UndoneAfter,
                Row0 = data[at..(at + had)], Row1 = data[afterAt..(afterAt + has)],
            });
            grown += has - had;
            at = afterAt + ((has + 3) & ~3);
        }
        return Math.Abs(data.Length - at) > 3 ? [c] : changes;
    }

    private static List<RowsetMove> Moves(List<Change> effective, TableLayout format)
    {
        var moves = new List<RowsetMove>();
        var at = format.Visible.First(c => c.Name == "idmajor").LeafOffset;
        foreach (var c in effective)
        {
            if (c.Table != CatalogTable.Rowsets || c.Image is not null || c.Raw.Row0 is not { } before || c.Raw.Row1 is not { } after) continue;
            int from = Math.Max(at, c.Raw.Offset), to = Math.Min(at + 4, c.Raw.Offset + Math.Min(before.Length, after.Length));
            if (from >= to || before.AsSpan(from - c.Raw.Offset, to - from).SequenceEqual(after.AsSpan(from - c.Raw.Offset, to - from))) continue;
            var whole = from == at && to == at + 4;
            moves.Add(new RowsetMove(c.Lsn, c.Rowset, whole ? BinaryPrimitives.ReadInt32LittleEndian(before.AsSpan(at - c.Raw.Offset)) : 0, whole ? BinaryPrimitives.ReadInt32LittleEndian(after.AsSpan(at - c.Raw.Offset)) : 0));
        }
        return moves;
    }

    /// <summary>
    /// LOP_MODIFY_COLUMNS: several separate places of one row changed at once. Contents 0 are the offsets (before and after the change) of each
    /// place, contents 1 the lengths, contents 4 the bytes: before and after of each, aligned to four (experiments/33). Comes out as one in-place
    /// change per place in the row's fixed part, which is all that layouts depend on; places in its variable part follow those.
    /// </summary>
    public static IEnumerable<CatalogChange> Expand(CatalogChange c, IReadOnlyDictionary<CatalogTable, TableLayout> formats)
    {
        if (c is { Operation: LogOperation.ModifyColumns, Compensation: false, Table: CatalogTable.Objects }) return ExpandObject(c, formats[c.Table]);
        if (c.Operation != LogOperation.ModifyColumns || c.Compensation || c.Table is not (CatalogTable.RowsetColumns or CatalogTable.Rowsets)) return [c];
        var data = c.Data ?? [];
        var fixedEnd = formats[c.Table].Columns.Where(x => x.LeafOffset >= 4).Max(x => x.LeafOffset + x.MaxInRowLength);
        CatalogHistoryException NotUnderstood() => new($"{c.Table} {c.Operation} at {c.Lsn} is not understood: {Convert.ToHexString(c.Row0 ?? [])} {Convert.ToHexString(c.Row1 ?? [])} {Convert.ToHexString(data)}");
        if (c.Row0 is not { Length: > 0 } offsets || c.Row1 is not { } lengths || offsets.Length % 4 != 0 || lengths.Length * 2 != offsets.Length) throw NotUnderstood();
        var changes = new List<CatalogChange>();
        var at = 0;
        for (var i = 0; i < lengths.Length / 2; i++)
        {
            var (before, after) = (BinaryPrimitives.ReadInt16LittleEndian(offsets.AsSpan(4 * i)), BinaryPrimitives.ReadInt16LittleEndian(offsets.AsSpan(4 * i + 2)));
            var length = BinaryPrimitives.ReadInt16LittleEndian(lengths.AsSpan(2 * i));
            if (before >= fixedEnd) break;
            var aligned = (length + 3) & ~3;
            if (before != after || before < 4 || length <= 0 || at + aligned + length > data.Length) throw NotUnderstood();
            changes.Add(new CatalogChange
            {
                Lsn = c.Lsn, Operation = LogOperation.ModifyRow, TransactionId = c.TransactionId, Table = c.Table, Offset = before, Key = c.Key, UndoneAfter = c.UndoneAfter,
                Row0 = data[at..(at + length)], Row1 = data[(at + aligned)..(at + aligned + length)],
            });
            at += 2 * aligned;
        }
        return changes;
    }

    /// <summary>Tables (object ids) that own the given rowsets according to the log (the rowsets may be gone from the catalog).</summary>
    public static IReadOnlySet<int> OwnersOf(IReadOnlySet<long> rowsets, IReadOnlyList<CatalogChange> changes, IReadOnlyDictionary<CatalogTable, TableLayout> formats)
    {
        var owners = new HashSet<int>();
        if (rowsets.Count == 0) return owners;
        foreach (var c in changes)
        {
            if (c.Table != CatalogTable.Rowsets || c.Compensation || c.Operation is not (LogOperation.InsertRows or LogOperation.DeleteRows)) continue;
            var d = Decode(c, formats);
            if (rowsets.Contains(d.Rowset)) owners.Add(d.ObjectId);
        }
        return owners;
    }

    /// <summary>Changes of committed user transactions, minus those undone by a partial rollback.</summary>
    private static List<CatalogChange> Effective(IReadOnlyList<CatalogChange> changes, IReadOnlyDictionary<ulong, TransactionOutcome> outcomes)
    {
        var kept = new List<CatalogChange>();
        foreach (var txn in changes.GroupBy(c => c.TransactionId))
        {
            var outcome = OutcomeOf(outcomes, txn.Key);
            if (outcome.IsSystem || !outcome.IsCommitted) continue;
            var alive = new List<CatalogChange>();
            foreach (var c in txn.OrderBy(c => c.Lsn))
            {
                if (!c.Compensation) { alive.Add(c); continue; }
                alive.RemoveAll(x => x.Lsn > c.UndoneAfter);
            }
            kept.AddRange(alive);
        }
        return [.. kept.OrderBy(c => c.Lsn)]; // a stable sort: the places of one record stay in their order
    }

    private static TransactionOutcome OutcomeOf(IReadOnlyDictionary<ulong, TransactionOutcome> outcomes, ulong xid) =>
        outcomes.TryGetValue(xid, out var o) ? o
            : throw new CatalogHistoryException($"transaction {LogRecord.FormatTransactionId(xid)} changed the catalog but its begin and end were not found in the log");

    /// <summary>An open transaction's rows must not be visible yet (the catalog is read committed): if they are, the two reads raced.</summary>
    private static void RefuseRacingChanges(CatalogSnapshot now, IReadOnlyList<CatalogChange> changes, IReadOnlyDictionary<ulong, TransactionOutcome> outcomes,
                                            IReadOnlyDictionary<CatalogTable, TableLayout> formats)
    {
        foreach (var c in changes)
        {
            if (c.Compensation || c.Table != CatalogTable.RowsetColumns || c.Operation != LogOperation.InsertRows) continue;
            if (OutcomeOf(outcomes, c.TransactionId) is not { IsSystem: false, IsOpen: true }) continue;
            var d = Decode(c, formats);
            if (now.Rowsets.TryGetValue(d.Rowset, out var r) && r.Columns.Any(x => x.RscolId == d.ColumnId && x.SameAs(PhysicalColumn.FromImage(d.Image!))))
                throw new CatalogHistoryException($"a DDL transaction on rowset {d.Rowset} committed while the catalog was being read");
        }
    }

    private static Change Decode(CatalogChange c, IReadOnlyDictionary<CatalogTable, TableLayout> formats)
    {
        if (c.Operation is LogOperation.InsertRows or LogOperation.DeleteRows)
        {
            if (c.Row0 is not { Length: > 0 }) throw new CatalogHistoryException($"{c.Table} {c.Operation} at {c.Lsn} carries no row image");
            RowImage image;
            try { image = RowDecoder.Decode(c.Row0, formats[c.Table], possiblyTruncated: c.Row0.Length == 8000); }
            catch (Exception ex) when (ex is RowFormatException or SchemaMismatchException)
            {
                throw new CatalogHistoryException($"{c.Table} row at {c.Lsn} does not fit the catalog's own layout: {ex.Message}", ex);
            }
            return c.Table switch
            {
                CatalogTable.RowsetColumns => new Change(c.Lsn, c.Operation, c.Table, image, c, Long(image, "rsid"), 0, Int(image, "rscolid")),
                CatalogTable.Rowsets => new Change(c.Lsn, c.Operation, c.Table, image, c, Long(image, "rowsetid"), Int(image, "idmajor"), 0),
                // number <> 0: parameters and the like
                CatalogTable.ObjectColumns => new Change(c.Lsn, c.Operation, c.Table, image, c, 0, Int(image, "number") == 0 ? Int(image, "id") : 0, Int(image, "colid")),
                CatalogTable.Objects => new Change(c.Lsn, c.Operation, c.Table, image, c, 0, Int(image, "id"), 0),
                _ => new Change(c.Lsn, c.Operation, c.Table, image, c, Long(image, "id"), 0, 0),
            };
        }
        // In-place change: only the clustered key is needed. sysrowsets is keyed by rowset id, sysrscols by rowset id and hbcolid, which
        // is the column's null bit position. In-place changes of names and defaults do not influence decoding.
        if (c.Table is not (CatalogTable.RowsetColumns or CatalogTable.Rowsets)) return new Change(c.Lsn, c.Operation, c.Table, null, c, 0, 0, 0);
        if (c.Key is not { } key || key.Length < (c.Table == CatalogTable.Rowsets ? 9 : 13)) throw new CatalogHistoryException($"{c.Table} {c.Operation} at {c.Lsn} carries no key");
        var column = c.Table == CatalogTable.RowsetColumns ? BinaryPrimitives.ReadInt32LittleEndian(key.AsSpan(9)) : 0;
        return new Change(c.Lsn, c.Operation, c.Table, null, c, BinaryPrimitives.ReadInt64LittleEndian(key.AsSpan(1)), 0, column);
    }

    private static List<LayoutVersion> Undo(Context x, long rowset, int objectId, List<Change> changes)
    {
        x.Now.Rowsets.TryGetValue(rowset, out var current);
        x.Now.Objects.TryGetValue(objectId, out var identity);
        var state = new State(
            current is null ? null : new RowsetRow(current.ObjectId, current.IndexId, current.Compression),
            current is null ? ImmutableDictionary<int, PhysicalColumn>.Empty : current.Columns.ToImmutableDictionary(c => c.RscolId),
            identity is null ? ImmutableDictionary<int, string>.Empty : identity.ColumnNames.ToImmutableDictionary());

        LayoutVersion Version(Lsn after, State s) => new(after, () => ToLayout(x, rowset, objectId, s, identity, current));

        var versions = new List<LayoutVersion>(changes.Count + 1);
        for (var i = changes.Count - 1; i >= 0; i--)
        {
            versions.Add(Version(changes[i].Lsn, state));
            state = UndoOne(state, changes[i], x.Formats, rowset);
        }
        versions.Add(Version(default, state));
        versions.Reverse();
        return versions;
    }

    private static State UndoOne(State s, Change c, IReadOnlyDictionary<CatalogTable, TableLayout> formats, long rowset)
    {
        CatalogHistoryException Contradiction(string what) => new($"rowset {rowset}: {c.Table} {c.Operation} at {c.Lsn}: {what}");
        switch (c.Table, c.Operation)
        {
            case (CatalogTable.RowsetColumns, LogOperation.InsertRows):
            {
                if (!s.Columns.TryGetValue(c.ColumnId, out var have)) throw Contradiction($"column {c.ColumnId} is not in the catalog");
                var image = PhysicalColumn.FromImage(c.Image!);
                if (!have.SameAs(image)) throw Contradiction($"column {c.ColumnId} differs: logged {image}, catalog {have}");
                return s with { Columns = s.Columns.Remove(c.ColumnId) };
            }
            case (CatalogTable.RowsetColumns, LogOperation.DeleteRows):
                if (s.Columns.ContainsKey(c.ColumnId)) throw Contradiction($"deleted column {c.ColumnId} is still in the catalog");
                return s with { Columns = s.Columns.Add(c.ColumnId, PhysicalColumn.FromImage(c.Image!)) };
            case (CatalogTable.RowsetColumns, LogOperation.ModifyRow):
            {
                if (!Matters(formats[c.Table], c.Raw)) return s;
                // The key says hbcolid, which the catalog views do not show. It is the null bit position, unless the column was altered to a type
                // of the same size (experiments/33): then its own row, if it is in the log, says both.
                var known = s.Columns.Values.Where(x => x.HbcolId == c.ColumnId).ToList();
                var old = known.Count == 1 ? known[0] : s.Columns.Values.FirstOrDefault(x => x.HbcolId == 0 && x.LeafNullBit == c.ColumnId) ?? throw Contradiction($"column {c.ColumnId} is not in the catalog");
                return s with { Columns = s.Columns.SetItem(old.RscolId, old.Undo(formats[c.Table], c.Raw.Offset, c.Raw.Row0 ?? [], c.Raw.Row1 ?? [])) };
            }
            case (CatalogTable.Rowsets, LogOperation.InsertRows):
                if (s.Row is null) throw Contradiction("the rowset is not in the catalog");
                if (s.Row != RowsetOf(c.Image!)) throw Contradiction($"logged {RowsetOf(c.Image!)}, catalog {s.Row}");
                return s with { Row = null };
            case (CatalogTable.Rowsets, LogOperation.DeleteRows):
                if (s.Row is not null) throw Contradiction("the dropped rowset is still in the catalog");
                return s with { Row = RowsetOf(c.Image!) };
            case (CatalogTable.Rowsets, LogOperation.ModifyRow):
            {
                if (s.Row is null) throw Contradiction("the rowset is not in the catalog");
                var fields = new Dictionary<string, int> { ["idmajor"] = s.Row.ObjectId, ["idminor"] = s.Row.IndexId, ["cmprlevel"] = (int)s.Row.Compression };
                UndoPatch(formats[c.Table], fields, c.Raw.Offset, c.Raw.Row0 ?? [], c.Raw.Row1 ?? [], unverified: null);
                return s with { Row = new RowsetRow(fields["idmajor"], fields["idminor"], (DataCompression)fields["cmprlevel"]) };
            }
            case (CatalogTable.ObjectColumns, LogOperation.InsertRows):
                return s with { Names = s.Names.Remove(c.ColumnId) };
            case (CatalogTable.ObjectColumns, LogOperation.DeleteRows):
                return s with { Names = s.Names.SetItem(c.ColumnId, (string)c.Image!["name"]!) };
            default:
                throw Contradiction("an in-place change of this kind is not understood");
        }
    }

    /// <summary>Bits of sysrscols.status that say whether the column is replicated, and whether it is logged for replication. They change when CDC is
    /// enabled or disabled and by SWITCH, and do not concern the layout.</summary>
    private const byte ReplicationBits = 0x05;

    /// <summary>Did the change touch anything that decoding depends on? (rcmodified, for one, changes all the time.)</summary>
    private static bool Matters(TableLayout format, CatalogChange c)
    {
        var (before, after) = (c.Row0 ?? [], c.Row1 ?? []);
        if (before.Length != after.Length) return true;
        for (var i = 0; i < before.Length; i++)
        {
            var changed = before[i] ^ after[i];
            if (changed == 0) continue;
            var column = format.Visible.FirstOrDefault(x => x.LeafOffset >= 4 && c.Offset + i >= x.LeafOffset && c.Offset + i < x.LeafOffset + x.MaxInRowLength);
            if (column is null) return true;
            if (column.Name is "rcmodified" or "ordlock") continue;
            if (column.Name == "status" && c.Offset + i == column.LeafOffset && (changed & ~ReplicationBits) == 0) continue;
            return true;
        }
        return false;
    }

    private static RowsetRow RowsetOf(RowImage r) => new(Int(r, "idmajor"), Int(r, "idminor"), (DataCompression)Int(r, "cmprlevel"));

    private static TableLayout? ToLayout(Context x, long rowset, int objectId, State s, TableIdentity? identity, CurrentRowset? current)
    {
        if (s.Row is null) return null;
        string schema, name;
        if (identity is not null) (schema, name) = (identity.Schema, identity.Name);
        else
        {
            (name, var schemaId) = x.DroppedTables[objectId];
            schema = x.Now.SchemaNames.GetValueOrDefault(schemaId) ?? string.Create(CultureInfo.InvariantCulture, $"schema#{schemaId}");
        }
        var columns = new List<ColumnLayout>(s.Columns.Count);
        foreach (var c in s.Columns.Values.OrderBy(c => c.RscolId))
        {
            var live = !c.IsDropped && !c.IsUniqueifier && !c.IsAntiMatter;
            var columnName = string.Create(CultureInfo.InvariantCulture, $"#col{c.RscolId}");
            if (live && !s.Names.TryGetValue(c.RscolId, out columnName))
                throw new CatalogHistoryException($"{schema}.{name} (rowset {rowset}): no name for column {c.RscolId}");
            // a dropped table's key is its clustered key (the index definitions are gone)
            var ordinal = !live ? 0 : identity is not null ? identity.KeyOrdinals.GetValueOrDefault(c.RscolId) : s.Row.IndexId == 1 ? c.KeyOrdinal : 0;
            object? def = null;
            // A column as the catalog has it: the catalog's default. One that the log tells of: the default is found by its hbcolid; if that is not
            // in the log, by its null bit, when the column is still there as a dropped one (what took its place has a default of its own).
            if (c.HbcolId == 0) current?.Defaults.TryGetValue(c.LeafNullBit, out def);
            else if (!x.Defaults.TryGetValue((rowset, c.HbcolId), out def) && current is not null && current.Columns.Any(k => k.LeafNullBit == c.LeafNullBit && (k.IsDropped || k.RscolId == c.RscolId && k.SameAs(c))))
                current.Defaults.TryGetValue(c.LeafNullBit, out def);
            columns.Add(ToColumn(c, columnName!, live ? c.RscolId : 0, c.CollationId == 0 ? 0 : x.Now.CodePage(c.CollationId), ordinal, def));
        }
        return new TableLayout(x.Now.DatabaseId, x.Now.Database, objectId, schema, name, rowset, s.Row.Compression, columns);
    }

    /// <summary>The one conversion from a physical column to a decoder layout.</summary>
    public static ColumnLayout ToColumn(PhysicalColumn c, string name, int columnId, int codePage, int keyOrdinal, object? defaultValue) => new()
    {
        ColumnId = columnId, PartitionColumnId = c.RscolId, Name = name, Type = SqlTypes.FromIds(c.XType, c.XType), MaxLength = c.Length, MaxInRowLength = c.MaxInRow,
        Precision = c.Precision, Scale = c.Scale, CodePage = codePage, LeafOffset = c.LeafOffset, LeafNullBit = c.LeafNullBit, LeafBitPosition = c.LeafBitPosition,
        IsNullable = c.IsNullable, IsDropped = c.IsDropped, IsUniqueifier = c.IsUniqueifier, IsSparse = c.IsSparse, KeyOrdinal = keyOrdinal, DefaultValue = defaultValue,
    };

    internal static int Int(RowImage r, string column) => Convert.ToInt32(r[column], CultureInfo.InvariantCulture);
    internal static long Long(RowImage r, string column) => Convert.ToInt64(r[column], CultureInfo.InvariantCulture);

    /// <summary>Undoes a byte-range change on the known integer fields of a catalog row: the overlapped bytes must hold the new value
    /// (otherwise the log and the catalog disagree) and get the old one back. Bytes of other fields do not matter.</summary>
    internal static void UndoPatch(TableLayout format, Dictionary<string, int> fields, short offset, byte[] before, byte[] after, string? unverified)
    {
        Span<byte> bytes = stackalloc byte[4];
        foreach (var name in fields.Keys.ToArray())
        {
            var col = format.Visible.FirstOrDefault(c => c.Name == name) ?? throw new CatalogHistoryException($"{format.Table} has no column {name}");
            var from = Math.Max(col.LeafOffset, offset);
            var to = Math.Min(col.LeafOffset + col.MaxInRowLength, offset + Math.Max(before.Length, after.Length));
            if (from >= to) continue;
            if (before.Length != after.Length)
                throw new CatalogHistoryException($"{format.Table}.{name}: a change in the fixed part of a row replaced {before.Length} with {after.Length} bytes");
            BinaryPrimitives.WriteInt32LittleEndian(bytes, fields[name]);
            for (var i = from; i < to; i++)
            {
                if (name != unverified && bytes[i - col.LeafOffset] != after[i - offset])
                    throw new CatalogHistoryException($"{format.Table}.{name}: logged 0x{after[i - offset]:X2}, catalog 0x{bytes[i - col.LeafOffset]:X2}");
                bytes[i - col.LeafOffset] = before[i - offset];
            }
            fields[name] = col.MaxInRowLength switch { 1 => bytes[0], 2 => BinaryPrimitives.ReadInt16LittleEndian(bytes), _ => BinaryPrimitives.ReadInt32LittleEndian(bytes) };
        }
    }

    /// <summary>Default of a column added without rewriting rows. Anything not understood is unavailable, never guessed.</summary>
    private static object? DecodeDefault(CatalogChange c, TableLayout format, Func<int, int> codePage)
    {
        try
        {
            var raw = RowDecoder.VariableColumn(c.Row0!, format.Visible.First(v => v.Name == "value"));
            return raw is null ? null : DecodeVariant(raw, codePage);
        }
        catch (Exception ex) when (ex is RowFormatException or ArgumentException or IndexOutOfRangeException or InvalidOperationException) { return Unavailable.Value; }
    }

    /// <summary>A stored sql_variant: [type][version 1][precision scale | scale | length [collation]][value] (experiments/13).</summary>
    internal static object DecodeVariant(ReadOnlySpan<byte> raw, Func<int, int> codePage)
    {
        if (raw.Length < 2 || raw[1] != 1) return Unavailable.Value;
        var type = SqlTypes.FromIds(raw[0], raw[0]);
        var data = raw[2..];
        byte precision = 0, scale = 0;
        var cp = 0;
        switch (type)
        {
            case SqlType.Bit: return data[0] != 0;
            case SqlType.Decimal:
            {
                precision = data[0];
                scale = data[1];
                var full = new byte[ValueDecoder.DecimalLength(precision)]; // the variant stores only the significant words of the magnitude
                data[2..].CopyTo(full);
                data = full;
                break;
            }
            case SqlType.Time or SqlType.DateTime2 or SqlType.DateTimeOffset: scale = data[0]; data = data[1..]; break;
            case SqlType.Char or SqlType.VarChar or SqlType.NChar or SqlType.NVarChar:
                cp = codePage(BinaryPrimitives.ReadInt32LittleEndian(data[2..]));
                data = data[6..];
                break;
            case SqlType.Binary or SqlType.VarBinary: data = data[2..]; break;
        }
        if (!SqlTypes.IsDecodable(type)) return Unavailable.Value;
        return ValueDecoder.Decode(data, new ColumnLayout
        {
            ColumnId = 1, PartitionColumnId = 1, Name = "default", Type = type, MaxLength = (short)data.Length, MaxInRowLength = (short)data.Length,
            Precision = precision, Scale = scale, CodePage = cp, LeafOffset = -1, LeafNullBit = 1,
        });
    }
}
