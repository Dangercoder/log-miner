using LogMiner.Rows;

namespace LogMiner.Schema;

/// <summary>
/// A physical column (one sys.sysrscols row) with the fields that decide how a row is decoded. Built from the log image or from
/// sys.system_internals_partition_columns; both must agree.
/// </summary>
public sealed record PhysicalColumn
{
    public const int StatusReplicated = 1, StatusDropped = 2, StatusLoggedForReplication = 4, StatusDescending = 8, StatusUniqueifier = 16, StatusFileStream = 32,
                     StatusAntiMatter = 64, StatusNotNull = 128, StatusSparse = 256;
    /// <summary>Status bits that influence decoding (the replication bits change when CDC is enabled or disabled on the table).</summary>
    private const int ComparedStatus = StatusDropped | StatusUniqueifier | StatusFileStream | StatusAntiMatter | StatusSparse;

    public required int RscolId { get; init; }
    /// <summary>Only known from log images (the view does not expose it); not compared.</summary>
    public int HbcolId { get; init; }
    public required byte XType { get; init; }
    public required short Length { get; init; }
    public byte Precision { get; init; }
    public byte Scale { get; init; }
    public required short MaxInRow { get; init; }
    public int CollationId { get; init; }
    public short KeyOrdinal { get; init; }
    public int Status { get; init; }
    public required int Offset { get; init; }
    public required int NullBit { get; init; }
    public short BitPosition { get; init; }

    public short LeafOffset => (short)(Offset & 0xFFFF);
    public short LeafNullBit => (short)(NullBit & 0xFFFF);
    public byte LeafBitPosition => (byte)(BitPosition & 0xFF);
    public bool IsDropped => (Status & StatusDropped) != 0;
    public bool IsUniqueifier => (Status & StatusUniqueifier) != 0;
    public bool IsAntiMatter => (Status & StatusAntiMatter) != 0;
    public bool IsSparse => (Status & StatusSparse) != 0;
    public bool IsNullable => (Status & StatusNotNull) == 0;

    /// <summary>Decode-relevant equality: type, size, position, flags.</summary>
    public bool SameAs(PhysicalColumn o) =>
        XType == o.XType && Length == o.Length && Precision == o.Precision && Scale == o.Scale && MaxInRow == o.MaxInRow && CollationId == o.CollationId
        && KeyOrdinal == o.KeyOrdinal && (Status & ComparedStatus) == (o.Status & ComparedStatus) && LeafOffset == o.LeafOffset && LeafNullBit == o.LeafNullBit
        && LeafBitPosition == o.LeafBitPosition;

    /// <summary>sysrscols.ti: type id in the low byte, then length, or precision and scale, or fractional-second scale.</summary>
    public int TypeInfo => XType switch
    {
        106 or 108 => XType | Precision << 8 | Scale << 16,
        41 or 42 or 43 => XType | Scale << 8,
        34 or 35 or 99 or 165 or 167 or 173 or 175 or 231 or 239 => XType | (Length < 0 ? 0 : (ushort)Length) << 8,
        _ => XType,
    };

    /// <summary>Type properties encoded in ti (what the RSCPROP rowset does inside the catalog view; experiments/12).</summary>
    public static (byte XType, short Length, byte Precision, byte Scale) DecodeTypeInfo(int ti)
    {
        var x = (byte)ti;
        switch (x)
        {
            case 106 or 108:
            {
                var p = (byte)(ti >> 8);
                return (x, (short)ValueDecoder.DecimalLength(p), p, (byte)(ti >> 16));
            }
            case 41 or 42 or 43:
            {
                var s = (byte)(ti >> 8);
                var time = ValueDecoder.TimeLength(s);
                return (x, (short)(x == 41 ? time : x == 42 ? time + 3 : time + 5), 0, s);
            }
            case 165 or 167 or 231:
            {
                var len = (short)(ti >> 8);
                return (x, len == 0 ? (short)-1 : len, 0, 0); // 0 = (max)
            }
            case 34 or 35 or 99 or 173 or 175 or 239: return (x, (short)(ti >> 8), 0, 0);
            default:
            {
                short len = x switch { 36 => 16, 40 => 3, 48 or 104 => 1, 52 => 2, 56 or 58 or 59 or 122 => 4, 60 or 61 or 62 or 127 or 189 => 8, 98 => 8016, 241 => -1, _ => 0 };
                return (x, len, 0, 0);
            }
        }
    }

    /// <summary>From the catalog view's columns (precision and scale are kept only where they are part of the stored type).</summary>
    public static PhysicalColumn FromView(int rscolId, byte xtype, short length, byte precision, byte scale, short maxInRow, int collationId, short keyOrdinal,
                                          int status, int offset, int nullBit, short bitPosition) => new()
    {
        RscolId = rscolId, XType = xtype, Length = length, MaxInRow = maxInRow, CollationId = collationId, KeyOrdinal = keyOrdinal, Status = status,
        Precision = xtype is 106 or 108 ? precision : (byte)0, Scale = xtype is 106 or 108 or 41 or 42 or 43 ? scale : (byte)0,
        Offset = offset, NullBit = nullBit, BitPosition = bitPosition,
    };

    internal static PhysicalColumn FromImage(RowImage r)
    {
        var (x, length, p, s) = DecodeTypeInfo(CatalogHistory.Int(r, "ti"));
        var maxInRow = (short)CatalogHistory.Int(r, "maxinrowlen");
        return new PhysicalColumn
        {
            RscolId = CatalogHistory.Int(r, "rscolid"), HbcolId = CatalogHistory.Int(r, "hbcolid"), XType = x, Length = length, Precision = p, Scale = s,
            MaxInRow = maxInRow == 0 ? length : maxInRow, CollationId = CatalogHistory.Int(r, "cid"), KeyOrdinal = (short)CatalogHistory.Int(r, "ordkey"),
            Status = CatalogHistory.Int(r, "status"), Offset = CatalogHistory.Int(r, "offset"), NullBit = CatalogHistory.Int(r, "nullbit"),
            BitPosition = (short)CatalogHistory.Int(r, "bitpos"),
        };
    }

    /// <summary>Undoes an in-place change: the bytes at <paramref name="offset"/> were <paramref name="before"/>.</summary>
    internal PhysicalColumn Undo(TableLayout format, short offset, byte[] before, byte[] after)
    {
        var fields = new Dictionary<string, int>
        {
            ["ti"] = TypeInfo, ["cid"] = CollationId, ["ordkey"] = KeyOrdinal, ["maxinrowlen"] = MaxInRow == Length ? 0 : MaxInRow, ["status"] = Status,
            ["offset"] = Offset, ["nullbit"] = NullBit, ["bitpos"] = BitPosition,
        };
        CatalogHistory.UndoPatch(format, fields, offset, before, after, unverified: "status"); // not all status bits are visible in the view
        var (x, length, p, s) = DecodeTypeInfo(fields["ti"]);
        return this with
        {
            XType = x, Length = length, Precision = p, Scale = s, MaxInRow = fields["maxinrowlen"] == 0 ? length : (short)fields["maxinrowlen"], CollationId = fields["cid"],
            KeyOrdinal = (short)fields["ordkey"], Status = fields["status"], Offset = fields["offset"], NullBit = fields["nullbit"], BitPosition = (short)fields["bitpos"],
        };
    }
}
