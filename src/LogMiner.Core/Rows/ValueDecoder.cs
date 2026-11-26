using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Text;
using LogMiner.Schema;

namespace LogMiner.Rows;

/// <summary>Decodes the on-disk (in-row) representation of a single column value.</summary>
public static class ValueDecoder
{
    private static readonly ConcurrentDictionary<int, Encoding> Encodings = new();
    private static readonly DateTime Epoch1900 = new(1900, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

    static ValueDecoder() => Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

    public static Encoding EncodingFor(int codePage) =>
        Encodings.GetOrAdd(codePage, cp => cp > 0 ? Encoding.GetEncoding(cp) : throw new RowFormatException($"unknown code page {cp}"));

    /// <summary>Exactly the text, or the bytes: text that does not encode to the same bytes again is not what is stored.</summary>
    public static object DecodeText(ReadOnlySpan<byte> raw, int codePage)
    {
        if (Ascii.IsValid(raw)) return Encoding.ASCII.GetString(raw); // the same in every code page SQL Server has
        var encoding = EncodingFor(codePage);
        var text = encoding.GetString(raw);
        return encoding.GetByteCount(text) == raw.Length && encoding.GetBytes(text).AsSpan().SequenceEqual(raw) ? text : new RawText(raw.ToArray());
    }

    /// <summary>
    /// Decodes <paramref name="raw"/> for <paramref name="column"/>. Fixed-length types require exactly
    /// their storage length. Returns CLR values: long/int/short/byte, bool, decimal (or string for
    /// precision beyond System.Decimal), double/float, DateOnly, TimeSpan, DateTime, DateTimeOffset,
    /// string, byte[], Guid.
    /// </summary>
    public static object Decode(ReadOnlySpan<byte> raw, ColumnLayout column)
    {
        switch (column.Type)
        {
            case SqlType.TinyInt: Need(raw, 1, column); return raw[0];
            case SqlType.SmallInt: Need(raw, 2, column); return BinaryPrimitives.ReadInt16LittleEndian(raw);
            case SqlType.Int: Need(raw, 4, column); return BinaryPrimitives.ReadInt32LittleEndian(raw);
            case SqlType.BigInt: Need(raw, 8, column); return BinaryPrimitives.ReadInt64LittleEndian(raw);
            case SqlType.Real: Need(raw, 4, column); return BinaryPrimitives.ReadSingleLittleEndian(raw);
            case SqlType.Float: Need(raw, 8, column); return BinaryPrimitives.ReadDoubleLittleEndian(raw);
            case SqlType.Decimal: return DecodeDecimal(raw, column);
            case SqlType.Money:
                // On disk a plain little-endian int64 of 1/10000 units (the TDS wire format swaps the halves; disk does not -
                // found by the real-engine round-trip test).
                Need(raw, 8, column);
                return FromScaledInteger(BinaryPrimitives.ReadInt64LittleEndian(raw), 4);
            case SqlType.SmallMoney: Need(raw, 4, column); return FromScaledInteger(BinaryPrimitives.ReadInt32LittleEndian(raw), 4);
            case SqlType.Date: Need(raw, 3, column); return Day(ReadUInt24(raw), column);
            case SqlType.Time:
                Need(raw, TimeLength(column.Scale), column);
                return TimeSpan.FromTicks(ReadTimeTicks(raw, column));
            case SqlType.DateTime2:
            {
                var tl = TimeLength(column.Scale);
                Need(raw, tl + 3, column);
                return Day(ReadUInt24(raw[tl..]), column).ToDateTime(TimeOnly.MinValue).AddTicks(ReadTimeTicks(raw[..tl], column));
            }
            case SqlType.DateTimeOffset:
            {
                var tl = TimeLength(column.Scale);
                Need(raw, tl + 5, column);
                var utc = Day(ReadUInt24(raw[tl..]), column).ToDateTime(TimeOnly.MinValue).AddTicks(ReadTimeTicks(raw[..tl], column));
                var minutes = BinaryPrimitives.ReadInt16LittleEndian(raw[(tl + 3)..]);
                if (minutes is < -840 or > 840) throw new RowFormatException($"column {column.Name}: a time zone offset of {minutes} minutes");
                try { return new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Unspecified) + TimeSpan.FromMinutes(minutes), TimeSpan.FromMinutes(minutes)); }
                catch (ArgumentOutOfRangeException ex) { throw new RowFormatException($"column {column.Name}: {utc:O} at {minutes} minutes is not a time", ex); }
            }
            case SqlType.DateTime:
            {
                Need(raw, 8, column);
                var ticks300 = BinaryPrimitives.ReadInt32LittleEndian(raw);
                var days = BinaryPrimitives.ReadInt32LittleEndian(raw[4..]);
                if (ticks300 is < 0 or >= 25_920_000 || days is < -53690 or > 2958463) throw new RowFormatException($"column {column.Name}: not a datetime ({days} days, {ticks300} ticks)");
                // 1/300 s ticks, rounded the way SQL Server presents them (to .000/.003/.007)
                var ms = (long)Math.Round(ticks300 * 10.0 / 3.0, MidpointRounding.AwayFromZero);
                return Epoch1900.AddDays(days).AddMilliseconds(ms);
            }
            case SqlType.SmallDateTime:
            {
                Need(raw, 4, column);
                var minutes = BinaryPrimitives.ReadUInt16LittleEndian(raw);
                var days = BinaryPrimitives.ReadUInt16LittleEndian(raw[2..]);
                if (minutes >= 1440) throw new RowFormatException($"column {column.Name}: {minutes} minutes in a day");
                return Epoch1900.AddDays(days).AddMinutes(minutes);
            }
            case SqlType.Char or SqlType.VarChar:
                return DecodeText(raw, column.CodePage);
            case SqlType.NChar or SqlType.NVarChar:
                if ((raw.Length & 1) != 0) throw new RowFormatException($"odd byte length {raw.Length} for {column.Name} ({column.Type})");
                return new string(MemoryMarshal.Cast<byte, char>(raw)); // also half a surrogate pair, which nvarchar may hold
            case SqlType.Binary or SqlType.VarBinary or SqlType.RowVersion or SqlType.HierarchyId or SqlType.Geometry or SqlType.Geography:
                return raw.ToArray();
            case SqlType.UniqueIdentifier: Need(raw, 16, column); return new Guid(raw);
            case SqlType.Bit:
                throw new InvalidOperationException("bit columns are decoded from the row's bit byte, not from a span");
            default:
                throw new RowFormatException($"type {column.Type} of {column.Name} is not decodable from in-row bytes");
        }
    }

    /// <summary>units / 10^scale as a decimal that keeps the scale (12.3400, like SQL Server presents money).</summary>
    public static decimal FromScaledInteger(long units, byte scale)
    {
        var magnitude = units == long.MinValue ? 9223372036854775808UL : (ulong)Math.Abs(units);
        return new decimal((int)(uint)magnitude, (int)(uint)(magnitude >> 32), 0, units < 0, scale);
    }

    /// <summary>Storage length of time/datetime2/datetimeoffset time part for a fractional-second scale.</summary>
    public static int TimeLength(byte scale) => scale <= 2 ? 3 : scale <= 4 ? 4 : 5;

    /// <summary>Storage length of decimal/numeric for a precision (sign byte + magnitude).</summary>
    public static int DecimalLength(byte precision) => precision <= 9 ? 5 : precision <= 19 ? 9 : precision <= 28 ? 13 : 17;

    private static object DecodeDecimal(ReadOnlySpan<byte> raw, ColumnLayout column)
    {
        var expected = DecimalLength(column.Precision);
        Need(raw, expected, column);
        if (raw[0] > 1) throw new RowFormatException($"column {column.Name}: sign byte 0x{raw[0]:X2}");
        var positive = raw[0] == 1;
        var mag = raw[1..];
        var w0 = BinaryPrimitives.ReadUInt32LittleEndian(mag);
        var w1 = mag.Length > 4 ? BinaryPrimitives.ReadUInt32LittleEndian(mag[4..]) : 0u;
        var w2 = mag.Length > 8 ? BinaryPrimitives.ReadUInt32LittleEndian(mag[8..]) : 0u;
        var w3 = mag.Length > 12 ? BinaryPrimitives.ReadUInt32LittleEndian(mag[12..]) : 0u;
        var magnitude = new BigInteger(mag, isUnsigned: true, isBigEndian: false);
        if (magnitude >= BigInteger.Pow(10, column.Precision)) throw new RowFormatException($"column {column.Name}: {magnitude} has more than {column.Precision} digits");
        if (w3 == 0 && column.Scale <= 28)
            return new decimal((int)w0, (int)w1, (int)w2, !positive, column.Scale);
        // beyond System.Decimal (magnitude > 96 bits or scale > 28): exact decimal string
        var digits = magnitude.ToString(System.Globalization.CultureInfo.InvariantCulture).PadLeft(column.Scale + 1, '0');
        var s = column.Scale == 0 ? digits : digits[..^column.Scale] + "." + digits[^column.Scale..];
        return positive ? s : "-" + s;
    }

    private static int ReadUInt24(ReadOnlySpan<byte> b) => b[0] | (b[1] << 8) | (b[2] << 16);

    private static DateOnly Day(int number, ColumnLayout column) =>
        number <= DateOnly.MaxValue.DayNumber ? DateOnly.FromDayNumber(number) : throw new RowFormatException($"column {column.Name}: day {number}");

    private static long ReadTimeTicks(ReadOnlySpan<byte> b, ColumnLayout column)
    {
        long units = 0;
        for (var i = b.Length - 1; i >= 0; i--) units = (units << 8) | b[i];
        for (var s = column.Scale; s < 7; s++) units *= 10;
        return units < TimeSpan.TicksPerDay ? units : throw new RowFormatException($"column {column.Name}: a time of {units} ticks");
    }

    private static void Need(ReadOnlySpan<byte> raw, int length, ColumnLayout column)
    {
        if (raw.Length != length)
            throw new RowFormatException($"column {column.Name} ({column.Type}) expects {length} bytes, got {raw.Length}");
    }
}

public sealed class RowFormatException : Exception
{
    public RowFormatException() { }
    public RowFormatException(string message) : base(message) { }
    public RowFormatException(string message, Exception inner) : base(message, inner) { }
}

/// <summary>Raised when a row image does not fit the cached layout (the schema changed): reload metadata.</summary>
public sealed class SchemaMismatchException : Exception
{
    public SchemaMismatchException() { }
    public SchemaMismatchException(string message) : base(message) { }
    public SchemaMismatchException(string message, Exception inner) : base(message, inner) { }
}
