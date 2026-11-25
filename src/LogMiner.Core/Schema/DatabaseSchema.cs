namespace LogMiner.Schema;

/// <summary>Physical layouts of a database's captured tables: the current ones, and (through <see cref="History"/>) the ones that
/// were valid earlier in the part of the log that is still to be read.</summary>
public sealed class DatabaseSchema
{
    private readonly Dictionary<long, TableLayout> _byRowset;
    private readonly IReadOnlySet<long> _notCaptured;

    /// <param name="databaseId">Database id.</param>
    /// <param name="database">Database name.</param>
    /// <param name="tables">Current layouts of captured tables, one per partition.</param>
    /// <param name="asOf">A log position read before the catalog: every DDL committed before it is reflected in the layouts.</param>
    /// <param name="untrackedTables">User tables that are not CDC-enabled.</param>
    /// <param name="history">Layouts as of earlier LSNs.</param>
    /// <param name="formerTables">Layouts of tables that still exist but are not captured any more; their logged changes still count.</param>
    /// <param name="notCaptured">Rowsets whose (replicated) records belong to someone else, e.g. transactional replication.</param>
    /// <param name="captureStarts">Per table (object id): from where on each of its CDC capture instances captures it.</param>
    public DatabaseSchema(int databaseId, string database, IReadOnlyList<TableLayout> tables, Lsn asOf = default, IReadOnlyList<string>? untrackedTables = null,
                          LayoutHistory? history = null, IReadOnlyList<TableLayout>? formerTables = null, IReadOnlySet<long>? notCaptured = null,
                          IReadOnlyDictionary<int, IReadOnlyList<Lsn>>? captureStarts = null)
    {
        DatabaseId = databaseId;
        Database = database;
        Tables = tables;
        AsOf = asOf;
        UntrackedTables = untrackedTables ?? [];
        History = history ?? LayoutHistory.None;
        _notCaptured = notCaptured ?? new HashSet<long>();
        CaptureStarts = captureStarts ?? new Dictionary<int, IReadOnlyList<Lsn>>();
        _byRowset = [];
        foreach (var t in formerTables ?? []) _byRowset[t.PartitionId] = t;
        foreach (var t in tables) _byRowset[t.PartitionId] = t;
    }

    public int DatabaseId { get; }
    public string Database { get; }
    public IReadOnlyList<TableLayout> Tables { get; }
    /// <summary>User tables in the database that are not CDC-enabled: their changes are NOT captured (operators must know).</summary>
    public IReadOnlyList<string> UntrackedTables { get; }
    public Lsn AsOf { get; }
    public IReadOnlyDictionary<int, IReadOnlyList<Lsn>> CaptureStarts { get; }
    public LayoutHistory History { get; }
    public DateTimeOffset LoadedAt { get; } = DateTimeOffset.UtcNow;

    /// <summary>Layout of a rowset (partition) for a record at <paramref name="lsn"/>; null when the rowset is unknown there.</summary>
    public TableLayout? Find(long rowsetId, Lsn lsn)
    {
        if (lsn < History.CoversFrom)
            throw new CatalogHistoryException($"{Database}: layouts were loaded for the log from {History.CoversFrom}, asked for {lsn}");
        if (!History.TryFind(rowsetId, lsn, out var layout)) layout = _byRowset.GetValueOrDefault(rowsetId);
        // under the name the table had when the change was made
        if (layout is null || History.Renames.Count == 0 || History.NameAt(layout.ObjectId, lsn) is not { } name || name == (layout.Schema, layout.Table)) return layout;
        if (!_renamed.TryGetValue((layout, name.Schema, name.Table), out var renamed))
            _renamed[(layout, name.Schema, name.Table)] = renamed = new TableLayout(layout.DatabaseId, layout.Database, layout.ObjectId, name.Schema, name.Table, layout.PartitionId, layout.Compression, layout.Columns);
        return renamed;
    }

    private readonly System.Collections.Concurrent.ConcurrentDictionary<(TableLayout, string, string), TableLayout> _renamed = new();

    /// <summary>Captured tables that were called something else since: by the layouts known before, or by the log behind those (after a restart, behind
    /// <paramref name="position"/>). Also one that has its old name back: what was changed in between came under the other.</summary>
    public IEnumerable<(TableLayout Table, Lsn At, string Was)> Renamed(DatabaseSchema? previous, Lsn position)
    {
        var known = previous?.AsOf ?? position;
        foreach (var t in Tables.DistinctBy(t => t.ObjectId))
        {
            var renames = History.Renames.Where(r => r.ObjectId == t.ObjectId && r.At > known).ToList();
            var names = renames.Select(r => $"{r.Schema}.{r.Table}");
            if (previous?.FindTable(t.ObjectId) is { } was && (was.Schema, was.Table) != (t.Schema, t.Table)) names = names.Prepend($"{was.Schema}.{was.Table}");
            var all = names.Distinct().ToList();
            if (all.Count > 0) yield return (t, renames.Count > 0 ? renames[0].At : AsOf, string.Join(", ", all));
        }
    }

    /// <summary>Current layout of a captured table (any partition), or null when it is not captured.</summary>
    public TableLayout? FindTable(int objectId) => Tables.FirstOrDefault(l => l.ObjectId == objectId);

    /// <summary>
    /// Tables whose capture started after a point where the stream already existed: newly enabled (rows from before were never
    /// published), or disabled and enabled again (changes in between were not captured at all). As long as one capture instance survives -
    /// e.g. a second one is created before the first is dropped - capture was continuous.
    /// </summary>
    /// <param name="previous">The layouts known before, or null after a restart.</param>
    /// <param name="position">After a restart: everything before this had been processed.</param>
    public IEnumerable<(TableLayout Table, Lsn From, bool Known)> CapturedLater(DatabaseSchema? previous, Lsn position)
    {
        foreach (var t in Tables.DistinctBy(t => t.ObjectId))
        {
            if (!CaptureStarts.TryGetValue(t.ObjectId, out var starts) || starts.Count == 0) continue;
            if (previous is null)
            {
                if (starts.All(s => s > position)) yield return (t, starts.Min(), false);
            }
            else if (!previous.CaptureStarts.TryGetValue(t.ObjectId, out var before)) yield return (t, starts.Min(), false);
            else if (!starts.Any(before.Contains)) yield return (t, starts.Min(), true);
        }
    }

    /// <summary>Captured tables that rows came into or went out of by SWITCH after <paramref name="position"/>, without any of them being logged.</summary>
    public IEnumerable<(TableLayout Table, Lsn At)> Switched(DatabaseSchema? previous, Lsn position)
    {
        var seen = new HashSet<(int, Lsn)>();
        foreach (var m in History.Moves.Where(m => m.At > position))
        {
            var tables = new[] { _byRowset.GetValueOrDefault(m.Rowset), previous?._byRowset.GetValueOrDefault(m.Rowset), FindTable(m.From), FindTable(m.To), previous?.FindTable(m.From) };
            foreach (var t in tables)
                if (t is not null && seen.Add((t.ObjectId, m.At))) yield return (t, m.At);
        }
    }

    /// <summary>DDL that changed values of rows that nobody wrote: converted columns, and columns that rows gained with a value. Consumers have to
    /// follow the DDL, or load the table again.</summary>
    public IEnumerable<string> Rewritten(DatabaseSchema? previous)
    {
        var before = (previous?.Tables ?? History.Earliest()).GroupBy(t => t.ObjectId).ToDictionary(g => g.Key, g => g.First());
        foreach (var now in Tables.DistinctBy(t => t.ObjectId))
        {
            if (!before.TryGetValue(now.ObjectId, out var was)) continue;
            foreach (var c in now.Visible)
            {
                var old = was.Visible.FirstOrDefault(x => x.ColumnId == c.ColumnId);
                if (old is null)
                {
                    if (!c.IsNullable || c.DefaultValue is not null) yield return $"{now}: column {c.Name} was added with a value for the rows that existed";
                }
                else if (old.Type != c.Type || old.Precision != c.Precision || old.Scale != c.Scale || old.CodePage != c.CodePage || (old.MaxLength != c.MaxLength && (c.Type is SqlType.Char or SqlType.NChar or SqlType.Binary)))
                    yield return $"{now}: the values of column {c.Name} were converted from {old.Type}({old.MaxLength},{old.Precision},{old.Scale}) to {c.Type}({c.MaxLength},{c.Precision},{c.Scale})";
            }
        }
    }

    /// <summary>Tables that were captured and are not any more.</summary>
    public IEnumerable<TableLayout> NoLongerCaptured(DatabaseSchema? previous) =>
        previous is null ? [] : previous.Tables.DistinctBy(t => t.ObjectId).Where(t => FindTable(t.ObjectId) is null);

    /// <summary>The rowset's replicated records are known to belong to someone else.</summary>
    public bool IsNotCaptured(long rowsetId) => _notCaptured.Contains(rowsetId);
}
