using System.Buffers.Binary;
using System.Text;

namespace LogMiner.Log;

/// <summary>A log block of a database reached its log file (extended event <c>log_flush_complete</c>).</summary>
/// <param name="Block">Position of the flushed log block (slot 0).</param>
public readonly record struct LogFlush(int DatabaseId, Lsn Block, uint WriteSize);

/// <summary>The live event stream does not look the way this version understands it.</summary>
public sealed class EventStreamFormatException(string message) : Exception(message);

/// <summary>
/// Decodes packets of SQL Server's live extended-event stream (<c>sys.fn_MSxe_read_event_stream</c>) of a session that holds nothing but
/// <c>log_flush_complete</c>. The packet framing is not documented, so every length, offset and id is checked and anything unexpected is an
/// <see cref="EventStreamFormatException"/> (the signal then polls instead); where the fields are comes from the documented
/// <c>sys.dm_xe_object_columns</c>.
/// <code>
/// packet 2 (once)  stream header: magic 5A37ABEF, u32 size 24
/// packet 1         metadata; every object is described as ... u32 id, name as UTF-16 ...
/// packet 0         events: u64 buffer, u32 sequence, u32 version, u32 length, u16 0, u16 first event, u32 length of events, u32 0, guid, ...
/// event            u32 id, u32 version, u32 0, u32 + u32 timestamp, u32 payload length, payload (fields in column order, unaligned)
/// </code>
/// </summary>
public sealed class LogFlushDecoder
{
    public const string EventName = "log_flush_complete";
    public const int HeaderPacket = 2, MetadataPacket = 1, EventPacket = 0;
    private const uint Magic = 0xEFAB375A;
    private const int Version = 10, PacketHeaderSize = 32, EventHeaderSize = 24;

    private readonly int _databaseId, _block, _writeSize, _payload;
    private uint _eventId;

    /// <param name="columns">Name and type of the event's data columns in column order (sys.dm_xe_object_columns).</param>
    public LogFlushDecoder(IEnumerable<(string Name, string Type)> columns)
    {
        _databaseId = _block = _writeSize = -1;
        foreach (var (name, type) in columns)
        {
            var size = type switch
            {
                "uint8" or "int8" or "boolean" => 1,
                "uint16" or "int16" => 2,
                "uint32" or "int32" or "float32" => 4,
                "uint64" or "int64" or "float64" or "ptr" => 8,
                _ => throw new EventStreamFormatException($"column {name} of {EventName} has the type {type}, which is not of fixed size"),
            };
            switch (name)
            {
                case "database_id": _databaseId = Expect(name, type, "uint32"); break;
                case "log_block_id": _block = Expect(name, type, "uint64"); break;
                case "write_size": _writeSize = Expect(name, type, "uint32"); break;
            }
            _payload += size;
        }
        if (_databaseId < 0 || _block < 0 || _writeSize < 0) throw new EventStreamFormatException($"{EventName} lacks one of database_id, log_block_id, write_size");
    }

    /// <summary>Events are understood from here on (the metadata packet was read).</summary>
    public bool Ready => _eventId != 0;

    /// <summary>Decodes one packet; flushes are appended to <paramref name="into"/>.</summary>
    public void Decode(int packetType, ReadOnlySpan<byte> packet, List<LogFlush> into)
    {
        switch (packetType)
        {
            case HeaderPacket:
                if (packet.Length < 8 || BinaryPrimitives.ReadUInt32LittleEndian(packet) != Magic) throw new EventStreamFormatException("unknown stream header");
                break;
            case MetadataPacket: ReadMetadata(packet); break;
            case EventPacket: ReadEvents(packet, into); break;
            default: throw new EventStreamFormatException($"unknown packet type {packetType}");
        }
    }

    /// <summary>The id the events carry stands in front of the event's name.</summary>
    private void ReadMetadata(ReadOnlySpan<byte> packet)
    {
        ReadOnlySpan<byte> name = Encoding.Unicode.GetBytes(EventName + "\0");
        var at = packet.IndexOf(name);
        if (at < 4) throw new EventStreamFormatException($"the stream's metadata does not describe {EventName}");
        if (packet[(at + name.Length)..].IndexOf(name) >= 0) throw new EventStreamFormatException($"the stream's metadata describes {EventName} more than once");
        var id = BinaryPrimitives.ReadUInt32LittleEndian(packet[(at - 4)..]);
        _eventId = id != 0 ? id : throw new EventStreamFormatException($"{EventName} has no id in the stream's metadata");
    }

    private void ReadEvents(ReadOnlySpan<byte> packet, List<LogFlush> into)
    {
        if (!Ready) throw new EventStreamFormatException("events before metadata");
        if (packet.Length < PacketHeaderSize) throw new EventStreamFormatException($"event packet of {packet.Length} bytes");
        if (BinaryPrimitives.ReadUInt32LittleEndian(packet[12..]) is var version and not Version) throw new EventStreamFormatException($"event packet version {version}");
        var length = BinaryPrimitives.ReadUInt32LittleEndian(packet[16..]);
        var first = BinaryPrimitives.ReadUInt16LittleEndian(packet[22..]);
        var events = BinaryPrimitives.ReadUInt32LittleEndian(packet[24..]);
        if (length != packet.Length || first < PacketHeaderSize || (ulong)first + events != length)
            throw new EventStreamFormatException($"event packet lengths {length}, {first}, {events} in {packet.Length} bytes");
        var rest = packet[first..];
        var start = into.Count;
        try
        {
            while (!rest.IsEmpty)
            {
                if (rest.Length < EventHeaderSize + _payload) throw new EventStreamFormatException($"{rest.Length} bytes left for an event");
                var id = BinaryPrimitives.ReadUInt32LittleEndian(rest);
                var eventVersion = BinaryPrimitives.ReadUInt32LittleEndian(rest[4..]);
                var payload = BinaryPrimitives.ReadUInt32LittleEndian(rest[20..]);
                if (id != _eventId || eventVersion != Version) throw new EventStreamFormatException($"event {id:X} version {eventVersion}, expected {_eventId:X}");
                if (payload != _payload) throw new EventStreamFormatException($"event payload of {payload} bytes, expected {_payload}");
                var p = rest.Slice(EventHeaderSize, _payload);
                var db = BinaryPrimitives.ReadUInt32LittleEndian(p[_databaseId..]);
                if (db is 0 or > 32767) throw new EventStreamFormatException($"database id {db}");
                var block = BinaryPrimitives.ReadUInt64LittleEndian(p[_block..]);
                into.Add(new LogFlush((int)db, new Lsn((uint)(block >> 32), (uint)block, 0), BinaryPrimitives.ReadUInt32LittleEndian(p[_writeSize..])));
                rest = rest[(EventHeaderSize + _payload)..];
            }
        }
        catch (EventStreamFormatException)
        {
            into.RemoveRange(start, into.Count - start); // all of a packet or nothing
            throw;
        }
    }

    private int Expect(string name, string type, string expected) =>
        type == expected ? _payload : throw new EventStreamFormatException($"column {name} of {EventName} is {type}, expected {expected}");
}
