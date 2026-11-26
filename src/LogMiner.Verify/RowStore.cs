using System.Buffers.Binary;
using System.IO.Hashing;

namespace LogMiner.Verify;

/// <summary>
/// The rows of one table, for millions of them: records back to back in a few large arrays (nothing per row for the garbage collector to
/// walk) and an open-addressing index of eight bytes per slot. A record is its epoch stamp, its shape (which columns, in the table's list of
/// shapes), its key as published and nine bytes per column. A narrow row takes 60 to 80 bytes, index included.
/// <para>A changed row is overwritten in place when its size is the same (the usual update); otherwise it is written anew and the old bytes are
/// reclaimed when they are more than what is alive.</para>
/// </summary>
internal sealed class RowStore
{
    private const int ChunkBits = 20, MaxChunk = 1 << ChunkBits, Header = 12, TagShift = 44;
    private const ulong Tomb = ulong.MaxValue, AddressMask = (1UL << TagShift) - 1;
    /// <summary>Keys longer than this are kept as a 128-bit hash: they still tell rows apart, a report cannot show them.</summary>
    public const int MaxKey = 4000;

    private List<byte[]> _chunks = [];
    private int _tail;
    private ulong[] _slots = new ulong[8];
    private int _count, _used;
    private long _live, _allocated, _hint;

    public int Count => _count;
    /// <summary>Memory held, index included.</summary>
    public long Bytes => _allocated + (long)_slots.Length * sizeof(ulong);
    public int Slots => _slots.Length;

    public static ulong Hash(ReadOnlySpan<byte> key) => XxHash3.HashToUInt64(key);

    private Span<byte> At(long address) => _chunks[(int)(address >> ChunkBits)].AsSpan((int)(address & (MaxChunk - 1)));

    public uint Stamp(long address) => BinaryPrimitives.ReadUInt32LittleEndian(At(address));
    public int Shape(long address) => BinaryPrimitives.ReadInt32LittleEndian(At(address)[4..]);
    public ReadOnlySpan<byte> Key(long address)
    {
        var r = At(address);
        return r.Slice(Header, BinaryPrimitives.ReadUInt16LittleEndian(r[8..]));
    }
    public ReadOnlySpan<byte> Cells(long address)
    {
        var r = At(address);
        return r.Slice(Header + BinaryPrimitives.ReadUInt16LittleEndian(r[8..]), BinaryPrimitives.ReadUInt16LittleEndian(r[10..]) * Canon.Cell);
    }
    private int Size(long address)
    {
        var r = At(address);
        return Header + BinaryPrimitives.ReadUInt16LittleEndian(r[8..]) + BinaryPrimitives.ReadUInt16LittleEndian(r[10..]) * Canon.Cell;
    }

    /// <summary>Where the key's row is, or -1.</summary>
    public long Find(ReadOnlySpan<byte> key, ulong hash) => Probe(key, hash, out _);

    private long Probe(ReadOnlySpan<byte> key, ulong hash, out int slot)
    {
        var mask = _slots.Length - 1;
        var tag = hash >> TagShift;
        var free = -1;
        for (var i = (int)hash & mask; ; i = (i + 1) & mask)
        {
            var s = _slots[i];
            if (s == 0) { slot = free < 0 ? i : free; return -1; }
            if (s == Tomb) { if (free < 0) free = i; continue; }
            if (s >> TagShift != tag) continue;
            var address = (long)(s & AddressMask) - 1;
            if (Key(address).SequenceEqual(key)) { slot = i; return address; }
        }
    }

    /// <summary>Sets the key's row.</summary>
    public void Put(ReadOnlySpan<byte> key, ulong hash, int shape, uint stamp, ReadOnlySpan<byte> cells)
    {
        var address = Probe(key, hash, out var slot);
        var size = Header + key.Length + cells.Length;
        if (address >= 0)
        {
            var old = Size(address);
            if (old == size) { Write(At(address), key, shape, stamp, cells); return; }
            _live -= old;
            _slots[slot] = hash >> TagShift << TagShift | (ulong)Append(key, shape, stamp, cells) + 1;
        }
        else
        {
            if (_slots[slot] == 0 && (_used + 1) * 4L > _slots.Length * 3L) { Rehash(_count + 1); Probe(key, hash, out slot); }
            if (_slots[slot] == 0) _used++; // a slot that was used before is used again
            _slots[slot] = hash >> TagShift << TagShift | (ulong)Append(key, shape, stamp, cells) + 1;
            _count++;
        }
        Trim();
    }

    public void SetStamp(long address, uint stamp) => BinaryPrimitives.WriteUInt32LittleEndian(At(address), stamp);

    public bool Remove(ReadOnlySpan<byte> key, ulong hash)
    {
        var address = Probe(key, hash, out var slot);
        if (address < 0) return false;
        RemoveAt(slot);
        Trim();
        return true;
    }

    /// <summary>The row in an index slot, or -1: for going over all rows. Rows may be removed on the way.</summary>
    public long AddressAt(int slot) => _slots[slot] is 0 or Tomb ? -1 : (long)(_slots[slot] & AddressMask) - 1;

    public void RemoveAt(int slot)
    {
        _live -= Size((long)(_slots[slot] & AddressMask) - 1);
        _slots[slot] = Tomb;
        _count--;
    }

    /// <summary>Gives back what removed rows held, when that is most of it. Not while going over the rows: they move.</summary>
    public void Trim()
    {
        if (_count == 0 && _allocated > 0) Clear();
        else if (_allocated > 2 * _live + (1 << 16)) Compact();
    }

    public void Clear()
    {
        _chunks = [];
        _slots = new ulong[8];
        _tail = _count = _used = 0;
        _live = _allocated = _hint = 0;
    }

    private static void Write(Span<byte> to, ReadOnlySpan<byte> key, int shape, uint stamp, ReadOnlySpan<byte> cells)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(to, stamp);
        BinaryPrimitives.WriteInt32LittleEndian(to[4..], shape);
        BinaryPrimitives.WriteUInt16LittleEndian(to[8..], (ushort)key.Length);
        BinaryPrimitives.WriteUInt16LittleEndian(to[10..], (ushort)(cells.Length / Canon.Cell));
        key.CopyTo(to[Header..]);
        cells.CopyTo(to[(Header + key.Length)..]);
    }

    private long Append(ReadOnlySpan<byte> key, int shape, uint stamp, ReadOnlySpan<byte> cells)
    {
        var size = Header + key.Length + cells.Length;
        if (_chunks.Count == 0 || _tail + size > _chunks[^1].Length)
        {
            // small tables stay small: 1,200 databases have many of them
            var length = Math.Max(size, (int)Math.Min(MaxChunk, Math.Max(_hint, _chunks.Count == 0 ? 256 : _chunks[^1].Length * 2L)));
            if (length > MaxChunk) throw new InvalidOperationException($"a row of {size} bytes of state cannot be kept");
            _chunks.Add(GC.AllocateUninitializedArray<byte>(length));
            _allocated += length;
            _tail = 0;
        }
        var address = (long)(_chunks.Count - 1) << ChunkBits | (uint)_tail;
        Write(_chunks[^1].AsSpan(_tail), key, shape, stamp, cells);
        _tail += size;
        _live += size;
        _hint = Math.Max(0, _hint - size);
        return address;
    }

    private void Rehash(int rows)
    {
        var old = _slots;
        _slots = new ulong[(int)System.Numerics.BitOperations.RoundUpToPowerOf2((uint)Math.Max(8, rows * 2L))];
        var mask = _slots.Length - 1;
        foreach (var s in old)
        {
            if (s is 0 or Tomb) continue;
            var i = (int)Hash(Key((long)(s & AddressMask) - 1)) & mask;
            while (_slots[i] != 0) i = (i + 1) & mask;
            _slots[i] = s;
        }
        _used = _count;
    }

    /// <summary>Copies what is alive into new arrays, in index order.</summary>
    private void Compact()
    {
        var (chunks, slots) = (_chunks, _slots);
        _chunks = [];
        // arrays of the size of what is alive: nothing is left over
        _hint = _live;
        _allocated = _live = 0;
        _tail = 0;
        for (var i = 0; i < slots.Length; i++)
        {
            if (slots[i] is 0 or Tomb) continue;
            var address = (long)(slots[i] & AddressMask) - 1;
            var r = chunks[(int)(address >> ChunkBits)].AsSpan((int)(address & (MaxChunk - 1)));
            int keyLength = BinaryPrimitives.ReadUInt16LittleEndian(r[8..]), cells = BinaryPrimitives.ReadUInt16LittleEndian(r[10..]) * Canon.Cell;
            var moved = Append(r.Slice(Header, keyLength), BinaryPrimitives.ReadInt32LittleEndian(r[4..]), BinaryPrimitives.ReadUInt32LittleEndian(r), r.Slice(Header + keyLength, cells));
            slots[i] = slots[i] >> TagShift << TagShift | (ulong)moved + 1;
        }
        _hint = 0;
        if (_used > _count * 2 + 8) Rehash(_count);
    }
}
