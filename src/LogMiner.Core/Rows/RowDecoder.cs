using System.Buffers.Binary;
using LogMiner.Schema;

namespace LogMiner.Rows;

/// <summary>Marker for a column whose value is not present in the log record (off-row LOB, truncated row, undecodable type).</summary>
public sealed class Unavailable
{
    public static readonly Unavailable Value = new();
    private Unavailable() { }
    public override string ToString() => "<unavailable>";
}

/// <summary>The bytes of a char or varchar value that is not valid text in its collation's code page (an invalid UTF-8 sequence, half a
/// double-byte character): decoding it as text would change it, and make different values equal.</summary>
public sealed record RawText(byte[] Bytes)
{
    public bool Equals(RawText? other) => other is not null && Bytes.AsSpan().SequenceEqual(other.Bytes);
    public override int GetHashCode() => Convert.ToHexString(Bytes).GetHashCode(StringComparison.Ordinal);
}

/// <summary>A decoded row: values aligned with <see cref="TableLayout.Visible"/>. SQL NULL is null; missing data is <see cref="Unavailable.Value"/>.</summary>
public sealed class RowImage(TableLayout layout, object?[] values)
{
    public TableLayout Layout { get; } = layout;
    public object?[] Values { get; } = values;

    public bool HasUnavailable => Array.IndexOf(Values, Unavailable.Value) >= 0;

    public object? this[string column] =>
        Layout.Visible.Select((c, i) => (c, i)).FirstOrDefault(t => t.c.Name == column) is var (col, idx) && col is not null ? Values[idx] : throw new KeyNotFoundException(column);
}

/// <summary>
/// Decodes SQL Server "FixedVar" data records (heap rows and clustered-index leaf rows):
/// <code>
/// [statusA][statusB][fixedEnd:2][fixed data ...][columnCount:2][null bitmap][varCount:2][varEnd:2 * varCount][var data ...][version tag:14?]
/// </code>
/// Variable-column end offsets with the high bit set are "complex" (off-row LOB / row-overflow pointers).
/// </summary>
public static class RowDecoder
{
    private const byte HasNullBitmap = 0x10;
    private const byte HasVariableColumns = 0x20;

    /// <summary>Decodes <paramref name="record"/>. When <paramref name="possiblyTruncated"/> is true (fn_dblog caps row log
    /// contents at 8000 bytes) columns lying past the end of the buffer become <see cref="Unavailable"/> instead of an error.</summary>
    public static RowImage Decode(ReadOnlySpan<byte> record, TableLayout layout, bool possiblyTruncated = false)
    {
        if (record.Length < 4) throw new RowFormatException($"record too short ({record.Length} bytes)");
        var statusA = record[0];
        var recordType = (statusA >> 1) & 0x7;
        if (recordType is not (0 or 1)) // 0 = primary record, 1 = forwarded record (heap)
            throw new RowFormatException($"not a data record (status 0x{statusA:X2}, type {recordType})");
        if ((statusA & HasNullBitmap) == 0)
            throw new RowFormatException($"data record without null bitmap (status 0x{statusA:X2})");

        int fixedEnd = BinaryPrimitives.ReadUInt16LittleEndian(record[2..]);
        if (fixedEnd < 4) throw new RowFormatException($"fixed-length end {fixedEnd} inside the header");
        if (fixedEnd + 2 > record.Length)
        {
            if (possiblyTruncated && fixedEnd <= MaxRowSize) return WithoutBitmap(record, layout);
            throw new RowFormatException($"fixed-length end {fixedEnd} outside record of {record.Length} bytes");
        }
        int columnCount = BinaryPrimitives.ReadUInt16LittleEndian(record[fixedEnd..]);
        var maxNullBit = 0;
        foreach (var c in layout.Columns) maxNullBit = Math.Max(maxNullBit, c.LeafNullBit);
        if (columnCount > maxNullBit)
            throw new SchemaMismatchException($"{layout}: row has {columnCount} columns, layout knows {maxNullBit}");

        var bitmapStart = fixedEnd + 2;
        var bitmapLength = (columnCount + 7) / 8;
        if (bitmapStart + bitmapLength > record.Length)
        {
            if (possiblyTruncated) return WithoutBitmap(record, layout);
            throw new RowFormatException("null bitmap past end of record");
        }
        var bitmap = record.Slice(bitmapStart, bitmapLength);

        var varCount = 0;
        var varArrayStart = bitmapStart + bitmapLength;
        if ((statusA & HasVariableColumns) != 0)
        {
            if (varArrayStart + 2 > record.Length)
            {
                if (possiblyTruncated) return WithoutBitmap(record, layout);
                throw new RowFormatException("variable column count past end of record");
            }
            varCount = BinaryPrimitives.ReadUInt16LittleEndian(record[varArrayStart..]);
            varArrayStart += 2;
            if (varCount > columnCount) throw new RowFormatException($"{varCount} variable columns in a row of {columnCount}");
            if (varArrayStart + 2 * varCount > record.Length)
            {
                if (possiblyTruncated) return WithoutBitmap(record, layout);
                throw new RowFormatException($"variable offset array ({varCount}) past end of record");
            }
        }
        var varDataStart = varArrayStart + 2 * varCount;

        var values = new object?[layout.Visible.Count];
        for (var i = 0; i < layout.Visible.Count; i++)
            values[i] = DecodeColumn(record, layout.Visible[i], fixedEnd, columnCount, bitmap, varCount, varArrayStart, varDataStart, possiblyTruncated);
        return new RowImage(layout, values);
    }

    private const int MaxRowSize = 8060;

    /// <summary>The log record was cut off within the row's fixed part, null bitmap or offsets. What is left tells the value of fixed columns
    /// that cannot be NULL; nothing can be said about the others.</summary>
    private static RowImage WithoutBitmap(ReadOnlySpan<byte> record, TableLayout layout)
    {
        var values = new object?[layout.Visible.Count];
        for (var i = 0; i < values.Length; i++)
        {
            var c = layout.Visible[i];
            values[i] = c.IsNullable || !c.IsFixed || c.Type == SqlType.Bit || !SqlTypes.IsDecodable(c.Type) || c.DefaultValue is not null || c.LeafOffset + c.MaxInRowLength > record.Length
                ? Unavailable.Value : ValueDecoder.Decode(record.Slice(c.LeafOffset, c.MaxInRowLength), c);
        }
        return new RowImage(layout, values);
    }

    private static object? DecodeColumn(ReadOnlySpan<byte> record, ColumnLayout col, int fixedEnd, int columnCount, ReadOnlySpan<byte> bitmap,
                                        int varCount, int varArrayStart, int varDataStart, bool possiblyTruncated)
    {
        // Column added after this row was written (metadata-only ALTER TABLE ADD): not stored in the row.
        if (col.LeafNullBit > columnCount)
            return col.DefaultValue is null && !col.IsNullable && !col.IsDropped
                ? throw new SchemaMismatchException($"column {col.Name} is not stored in the row, cannot be NULL, and its default is not known") : col.DefaultValue;
        var nullIndex = col.LeafNullBit - 1;
        if ((bitmap[nullIndex >> 3] & (1 << (nullIndex & 7))) != 0) return null;

        if (col.Type == SqlType.Bit)
        {
            if (col.LeafOffset < 4 || col.LeafOffset >= fixedEnd) throw new SchemaMismatchException($"column {col.Name} at {col.LeafOffset} lies outside the row's fixed part ({fixedEnd})");
            return (record[col.LeafOffset] & (1 << col.LeafBitPosition)) != 0;
        }

        if (col.IsFixed)
        {
            if (col.LeafOffset < 4 || col.LeafOffset + col.MaxInRowLength > fixedEnd) throw new SchemaMismatchException($"column {col.Name} at {col.LeafOffset} lies outside the row's fixed part ({fixedEnd})");
            return SqlTypes.IsDecodable(col.Type)
                ? ValueDecoder.Decode(record.Slice(col.LeafOffset, col.MaxInRowLength), col)
                : Unavailable.Value;
        }

        var index = -col.LeafOffset; // 1-based slot in the variable column array
        // Trailing zero-length variable columns are not stored. NULL was ruled out by the bitmap above, so a missing trailing
        // column is an empty value ('' / 0x), not NULL (found by the real-engine round-trip test).
        if (index > varCount) return SqlTypes.IsDecodable(col.Type) ? ValueDecoder.Decode([], col) : Unavailable.Value;
        int end = BinaryPrimitives.ReadUInt16LittleEndian(record[(varArrayStart + 2 * (index - 1))..]);
        var start = index == 1 ? varDataStart : BinaryPrimitives.ReadUInt16LittleEndian(record[(varArrayStart + 2 * (index - 2))..]) & 0x7FFF;
        var complex = (end & 0x8000) != 0;
        end &= 0x7FFF;
        if (end < start) throw new RowFormatException($"column {col.Name}: end offset {end} before start {start}");
        if (end > record.Length)
        {
            if (possiblyTruncated) return Unavailable.Value;
            throw new RowFormatException($"column {col.Name}: end offset {end} past record of {record.Length} bytes");
        }
        if (complex || !SqlTypes.IsDecodable(col.Type)) return Unavailable.Value;
        return ValueDecoder.Decode(record[start..end], col);
    }

    /// <summary>Raw bytes of an in-row variable-length column (null when NULL).</summary>
    internal static byte[]? VariableColumn(ReadOnlySpan<byte> record, ColumnLayout col)
    {
        int fixedEnd = BinaryPrimitives.ReadUInt16LittleEndian(record[2..]);
        int columnCount = BinaryPrimitives.ReadUInt16LittleEndian(record[fixedEnd..]);
        var bitmap = record[(fixedEnd + 2)..];
        var nullIndex = col.LeafNullBit - 1;
        if (col.LeafNullBit > columnCount || (bitmap[nullIndex >> 3] & (1 << (nullIndex & 7))) != 0 || (record[0] & HasVariableColumns) == 0) return null;
        var array = fixedEnd + 2 + (columnCount + 7) / 8;
        int varCount = BinaryPrimitives.ReadUInt16LittleEndian(record[array..]);
        var index = -col.LeafOffset;
        if (index > varCount) return [];
        int end = BinaryPrimitives.ReadUInt16LittleEndian(record[(array + 2 * index)..]);
        if ((end & 0x8000) != 0) throw new RowFormatException($"column {col.Name} is stored off-row");
        var start = index == 1 ? array + 2 + 2 * varCount : BinaryPrimitives.ReadUInt16LittleEndian(record[(array + 2 * (index - 1))..]) & 0x7FFF;
        return record[start..end].ToArray();
    }

    /// <summary>Total length the record claims (end of last variable column, or end of null bitmap), excluding a version tag.</summary>
    public static int DeclaredLength(ReadOnlySpan<byte> record)
    {
        if (record.Length < 4) return record.Length;
        int fixedEnd = BinaryPrimitives.ReadUInt16LittleEndian(record[2..]);
        if (fixedEnd + 2 > record.Length) return int.MaxValue;
        int columnCount = BinaryPrimitives.ReadUInt16LittleEndian(record[fixedEnd..]);
        var pos = fixedEnd + 2 + (columnCount + 7) / 8;
        if ((record[0] & HasVariableColumns) == 0) return pos;
        if (pos + 2 > record.Length) return int.MaxValue;
        int varCount = BinaryPrimitives.ReadUInt16LittleEndian(record[pos..]);
        if (varCount == 0) return pos + 2;
        var lastEndPos = pos + 2 * varCount;
        if (lastEndPos + 2 > record.Length) return int.MaxValue;
        return BinaryPrimitives.ReadUInt16LittleEndian(record[lastEndPos..]) & 0x7FFF;
    }
}
