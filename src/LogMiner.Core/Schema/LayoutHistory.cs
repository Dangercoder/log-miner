namespace LogMiner.Schema;

/// <summary>The layout of a rowset for records with an LSN above <see cref="After"/> (up to the next version).
/// <see cref="Layout"/> null: the rowset did not exist then.</summary>
public sealed class LayoutVersion(Lsn after, Func<TableLayout?> layout)
{
    // built on first use: most versions are never asked for, and intermediate states of one DDL statement never are
    private readonly Lazy<TableLayout?> _layout = new(layout);

    public LayoutVersion(Lsn after, TableLayout? layout) : this(after, () => layout) { }

    public Lsn After { get; } = after;
    public TableLayout? Layout => _layout.Value;
}

/// <summary>
/// Layouts as of any LSN at or after <see cref="CoversFrom"/>, for rowsets whose layout changed in that range. The log being read
/// is often older than the catalog (the miner was down, the sink was out, a migration ran faster than the miner): a record must be
/// decoded with the layout it was written with, not with today's.
/// </summary>
public sealed class LayoutHistory
{
    public static readonly LayoutHistory None = new(default, new Dictionary<long, IReadOnlyList<LayoutVersion>>());
    private readonly IReadOnlyDictionary<long, IReadOnlyList<LayoutVersion>> _rowsets;

    /// <param name="coversFrom">Oldest LSN the history answers for.</param>
    /// <param name="rowsets">Versions per rowset in ascending <see cref="LayoutVersion.After"/> order; the first is valid from <paramref name="coversFrom"/>.</param>
    /// <param name="moves">Rowsets that changed hands within the covered range.</param>
    public LayoutHistory(Lsn coversFrom, IReadOnlyDictionary<long, IReadOnlyList<LayoutVersion>> rowsets, IReadOnlyList<RowsetMove>? moves = null)
    {
        CoversFrom = coversFrom;
        _rowsets = rowsets;
        Moves = moves ?? [];
    }

    public IReadOnlyList<RowsetMove> Moves { get; }

    /// <summary>The layouts valid at <see cref="CoversFrom"/> of rowsets that changed since.</summary>
    public IEnumerable<TableLayout> Earliest()
    {
        foreach (var versions in _rowsets.Values)
        {
            TableLayout? layout;
            try { layout = versions[0].Layout; }
            catch (CatalogHistoryException) { continue; }
            if (layout is not null) yield return layout;
        }
    }

    public Lsn CoversFrom { get; }
    /// <summary>Rowsets whose layout changed within the covered range.</summary>
    public IEnumerable<long> Rowsets => _rowsets.Keys;

    /// <summary>False when the rowset did not change in the covered range (its current layout applies).</summary>
    public bool TryFind(long rowsetId, Lsn lsn, out TableLayout? layout)
    {
        layout = null;
        if (!_rowsets.TryGetValue(rowsetId, out var versions)) return false;
        for (var i = versions.Count - 1; i >= 0; i--)
        {
            if (i > 0 && lsn <= versions[i].After) continue;
            layout = versions[i].Layout;
            return true;
        }
        return false;
    }
}

/// <summary>ALTER TABLE ... SWITCH: a rowset with all its rows went from one table to another, without a record of any row.
/// <see cref="From"/> and <see cref="To"/> are object ids, 0 when the log does not say.</summary>
public sealed record RowsetMove(Lsn At, long Rowset, int From, int To);

/// <summary>The catalog and the log do not fit together: a DDL raced with the catalog read (retry) or the engine logs its catalog in a
/// way this version does not understand (fail closed; never guess a layout).</summary>
public sealed class CatalogHistoryException : Exception
{
    public CatalogHistoryException() { }
    public CatalogHistoryException(string message) : base(message) { }
    public CatalogHistoryException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>A committed change of a captured table cannot be attributed to a layout. The stream stops (nothing is skipped silently);
/// see docs/design.md for the recovery choices.</summary>
public sealed class UndecodableChangeException : Exception
{
    public UndecodableChangeException() { }
    public UndecodableChangeException(string message) : base(message) { }
    public UndecodableChangeException(string message, Exception inner) : base(message, inner) { }
}
