using System.Buffers.Binary;
using System.Globalization;

namespace LogMiner;

/// <summary>
/// SQL Server log sequence number: virtual log file sequence, log block offset, slot.
/// Ordering is lexicographic on (Vlf, Block, Slot), matching the engine.
/// Text form is fn_dblog's "0000002A:000006C8:0003"; binary form is the 10-byte big-endian
/// value used by sp_repldone and the cdc.* functions.
/// </summary>
public readonly record struct Lsn(uint Vlf, uint Block, ushort Slot) : IComparable<Lsn>, ISpanFormattable
{
    public const int BinarySize = 10;
    public static readonly Lsn Zero;

    public bool IsZero => Vlf == 0 && Block == 0 && Slot == 0;

    public int CompareTo(Lsn other)
    {
        var c = Vlf.CompareTo(other.Vlf);
        if (c != 0) return c;
        c = Block.CompareTo(other.Block);
        return c != 0 ? c : Slot.CompareTo(other.Slot);
    }

    public static bool operator <(Lsn a, Lsn b) => a.CompareTo(b) < 0;
    public static bool operator >(Lsn a, Lsn b) => a.CompareTo(b) > 0;
    public static bool operator <=(Lsn a, Lsn b) => a.CompareTo(b) <= 0;
    public static bool operator >=(Lsn a, Lsn b) => a.CompareTo(b) >= 0;

    public static Lsn Max(Lsn a, Lsn b) => a >= b ? a : b;
    public static Lsn Min(Lsn a, Lsn b) => a <= b ? a : b;

    /// <summary>Parses fn_dblog's hex form "VVVVVVVV:BBBBBBBB:SSSS" (an optional "0x" prefix is accepted).</summary>
    public static Lsn Parse(ReadOnlySpan<char> s) =>
        TryParse(s, out var lsn) ? lsn : throw new FormatException($"invalid LSN '{s}'");

    public static bool TryParse(ReadOnlySpan<char> s, out Lsn lsn)
    {
        lsn = default;
        if (s.StartsWith("0x", StringComparison.OrdinalIgnoreCase)) s = s[2..];
        if (s.Length != 22 || s[8] != ':' || s[17] != ':') return false;
        if (!uint.TryParse(s[..8], NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out var vlf)) return false;
        if (!uint.TryParse(s[9..17], NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out var block)) return false;
        if (!ushort.TryParse(s[18..], NumberStyles.AllowHexSpecifier, CultureInfo.InvariantCulture, out var slot)) return false;
        lsn = new Lsn(vlf, block, slot);
        return true;
    }

    public static Lsn FromBinary(ReadOnlySpan<byte> b)
    {
        if (b.Length != BinarySize) throw new ArgumentException("LSN must be 10 bytes", nameof(b));
        return new Lsn(BinaryPrimitives.ReadUInt32BigEndian(b), BinaryPrimitives.ReadUInt32BigEndian(b[4..]), BinaryPrimitives.ReadUInt16BigEndian(b[8..]));
    }

    public byte[] ToBinary()
    {
        var b = new byte[BinarySize];
        WriteBinary(b);
        return b;
    }

    public void WriteBinary(Span<byte> destination)
    {
        BinaryPrimitives.WriteUInt32BigEndian(destination, Vlf);
        BinaryPrimitives.WriteUInt32BigEndian(destination[4..], Block);
        BinaryPrimitives.WriteUInt16BigEndian(destination[8..], Slot);
    }

    /// <summary>Decimal "vlf:block:slot" form accepted by fn_dblog / fn_full_dblog @start/@end.</summary>
    public string ToDecimalString() => string.Create(CultureInfo.InvariantCulture, $"{Vlf}:{Block}:{Slot}");

    public override string ToString() => string.Create(CultureInfo.InvariantCulture, $"{Vlf:X8}:{Block:X8}:{Slot:X4}");

    public string ToString(string? format, IFormatProvider? formatProvider) => ToString();

    public bool TryFormat(Span<char> destination, out int charsWritten, ReadOnlySpan<char> format, IFormatProvider? provider) =>
        destination.TryWrite(CultureInfo.InvariantCulture, $"{Vlf:X8}:{Block:X8}:{Slot:X4}", out charsWritten);
}
