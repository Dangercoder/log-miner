namespace LogMiner.Schema;

/// <summary>SQL Server storage types the decoder understands (mapped from system_type_id / user_type_id).</summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage("Naming", "CA1720", Justification = "Members deliberately mirror SQL Server type names.")]
public enum SqlType : byte
{
    Unknown = 0,
    TinyInt, SmallInt, Int, BigInt, Bit,
    Decimal, Money, SmallMoney, Float, Real,
    Date, Time, DateTime2, DateTimeOffset, DateTime, SmallDateTime,
    Char, VarChar, NChar, NVarChar, Binary, VarBinary,
    UniqueIdentifier, RowVersion,
    Xml, Text, NText, Image, SqlVariant, HierarchyId, Geometry, Geography,
}

public static class SqlTypes
{
    /// <summary>Maps sys.types ids to <see cref="SqlType"/>. CLR types (system_type_id 240) are told apart by user_type_id.</summary>
    public static SqlType FromIds(int systemTypeId, int userTypeId) => systemTypeId switch
    {
        34 => SqlType.Image,
        35 => SqlType.Text,
        36 => SqlType.UniqueIdentifier,
        40 => SqlType.Date,
        41 => SqlType.Time,
        42 => SqlType.DateTime2,
        43 => SqlType.DateTimeOffset,
        48 => SqlType.TinyInt,
        52 => SqlType.SmallInt,
        56 => SqlType.Int,
        58 => SqlType.SmallDateTime,
        59 => SqlType.Real,
        60 => SqlType.Money,
        61 => SqlType.DateTime,
        62 => SqlType.Float,
        98 => SqlType.SqlVariant,
        99 => SqlType.NText,
        104 => SqlType.Bit,
        106 or 108 => SqlType.Decimal,
        122 => SqlType.SmallMoney,
        127 => SqlType.BigInt,
        165 => SqlType.VarBinary,
        167 => SqlType.VarChar,
        173 => SqlType.Binary,
        175 => SqlType.Char,
        189 => SqlType.RowVersion,
        231 => SqlType.NVarChar,
        239 => SqlType.NChar,
        240 => userTypeId switch { 128 => SqlType.HierarchyId, 129 => SqlType.Geometry, 130 => SqlType.Geography, _ => SqlType.Unknown },
        241 => SqlType.Xml,
        _ => SqlType.Unknown,
    };

    /// <summary>Types whose value can be decoded from in-row bytes (complex/off-row values are handled separately).</summary>
    public static bool IsDecodable(SqlType t) => t is not (SqlType.Unknown or SqlType.Xml or SqlType.SqlVariant or SqlType.Text or SqlType.NText or SqlType.Image);
}

/// <summary>
/// One physical column of a rowset (heap or clustered index leaf), as described by
/// sys.system_internals_partition_columns. Dropped columns stay in the physical layout until a rebuild.
/// </summary>
public sealed record ColumnLayout
{
    /// <summary>sys.columns.column_id; 0 for dropped/internal columns that have no logical column.</summary>
    public required int ColumnId { get; init; }
    public required int PartitionColumnId { get; init; }
    public required string Name { get; init; }
    public required SqlType Type { get; init; }
    /// <summary>Declared max length in bytes (-1 for (max) types).</summary>
    public required short MaxLength { get; init; }
    public required short MaxInRowLength { get; init; }
    public byte Precision { get; init; }
    public byte Scale { get; init; }
    /// <summary>Windows code page of a char/varchar column's collation (65001 for _UTF8 collations).</summary>
    public int CodePage { get; init; }
    /// <summary>&gt; 0: byte offset of a fixed-length column; &lt; 0: 1-based index into the variable-length column array.</summary>
    public required short LeafOffset { get; init; }
    /// <summary>1-based position in the null bitmap.</summary>
    public required short LeafNullBit { get; init; }
    public byte LeafBitPosition { get; init; }
    public bool IsNullable { get; init; }
    public bool IsDropped { get; init; }
    public bool IsUniqueifier { get; init; }
    public bool IsSparse { get; init; }
    /// <summary>1-based position in the clustered/primary key, 0 when not a key column.</summary>
    public int KeyOrdinal { get; init; }
    /// <summary>Value of a column added with a default after rows were written (metadata-only add).</summary>
    public object? DefaultValue { get; init; }

    public bool IsFixed => LeafOffset > 0;
    public bool IsVisible => !IsDropped && !IsUniqueifier && ColumnId > 0;
}

public enum DataCompression : byte { None = 0, Row = 1, Page = 2, ColumnStore = 3 }

/// <summary>Physical layout of one partition of a captured table.</summary>
public sealed class TableLayout
{
    public TableLayout(int databaseId, string database, int objectId, string schema, string table, long partitionId,
                       DataCompression compression, IReadOnlyList<ColumnLayout> columns)
    {
        DatabaseId = databaseId;
        Database = database;
        ObjectId = objectId;
        Schema = schema;
        Table = table;
        PartitionId = partitionId;
        Compression = compression;
        Columns = columns;
        Visible = columns.Where(c => c.IsVisible).OrderBy(c => c.ColumnId).ToArray();
        Key = Visible.Where(c => c.KeyOrdinal > 0).OrderBy(c => c.KeyOrdinal).ToArray();
        VisibleIndex = new Dictionary<int, int>();
        for (var i = 0; i < Visible.Count; i++) VisibleIndex[Visible[i].ColumnId] = i;
    }

    public int DatabaseId { get; }
    public string Database { get; }
    public int ObjectId { get; }
    public string Schema { get; }
    public string Table { get; }
    public long PartitionId { get; }
    public DataCompression Compression { get; }

    /// <summary>All physical columns, including dropped ones (they still occupy space in old rows).</summary>
    public IReadOnlyList<ColumnLayout> Columns { get; }

    /// <summary>User-visible columns in column_id order: the shape of before/after images.</summary>
    public IReadOnlyList<ColumnLayout> Visible { get; }

    /// <summary>Primary/clustered key columns in key order (empty for heaps without a declared key).</summary>
    public IReadOnlyList<ColumnLayout> Key { get; }

    internal Dictionary<int, int> VisibleIndex { get; }

    /// <summary>Position of a column (by column_id) in <see cref="Visible"/> / row images.</summary>
    public int IndexOf(int columnId) => VisibleIndex[columnId];

    /// <summary>Why rows of this table cannot be decoded, or null when supported.</summary>
    public string? UnsupportedReason =>
        Compression != DataCompression.None ? $"{Compression} compression is not supported yet"
        : Columns.Any(c => c.IsSparse && !c.IsDropped) ? "sparse columns are not supported yet"
        : null;

    public override string ToString() => $"{Database}.{Schema}.{Table} (partition {PartitionId})";
}
