using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using System.Numerics;
using System.Text.Json;
using Hegel;
using Hegel.Xunit;
using LogMiner.Capture;
using LogMiner.Pipeline;
using LogMiner.Sinks;
using LogMiner.SqlServer;
using Microsoft.Data.SqlClient;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>
/// The real engine is the oracle. Random DML (every supported type, NULLs, multi-row transactions, savepoint rollbacks, aborts)
/// runs against a fresh TDE + CDC database; the miner reads the log through fn_full_dblog, decodes, and publishes JSON events.
/// A replica rebuilt only from those events must equal <c>SELECT *</c> from the table after every step - also across log backups
/// with sp_repldone and capture restarts from checkpoints.
/// </summary>
public class EngineRoundTripProperties
{
    private sealed record Col(string Name, string SqlType, SqlDbType DbType, int Size = 0, byte Precision = 0, byte Scale = 0);

    private static readonly Col[] Cols =
    [
        new("c_tinyint", "tinyint", SqlDbType.TinyInt), new("c_smallint", "smallint", SqlDbType.SmallInt), new("c_int", "int", SqlDbType.Int),
        new("c_bigint", "bigint", SqlDbType.BigInt), new("c_bit", "bit", SqlDbType.Bit), new("c_bit2", "bit", SqlDbType.Bit),
        new("c_dec", "decimal(18,4)", SqlDbType.Decimal, 0, 18, 4), new("c_dec38", "decimal(38,10)", SqlDbType.Decimal, 0, 38, 10),
        new("c_num", "numeric(5,2)", SqlDbType.Decimal, 0, 5, 2), new("c_money", "money", SqlDbType.Money), new("c_smallmoney", "smallmoney", SqlDbType.SmallMoney),
        new("c_float", "float", SqlDbType.Float), new("c_real", "real", SqlDbType.Real),
        new("c_date", "date", SqlDbType.Date), new("c_time", "time(7)", SqlDbType.Time, 0, 0, 7), new("c_time0", "time(0)", SqlDbType.Time, 0, 0, 0),
        new("c_dt2", "datetime2(7)", SqlDbType.DateTime2, 0, 0, 7), new("c_dt2_3", "datetime2(3)", SqlDbType.DateTime2, 0, 0, 3),
        new("c_dto", "datetimeoffset(7)", SqlDbType.DateTimeOffset, 0, 0, 7), new("c_dt", "datetime", SqlDbType.DateTime), new("c_sdt", "smalldatetime", SqlDbType.SmallDateTime),
        new("c_char", "char(10)", SqlDbType.Char, 10), new("c_varchar", "varchar(100)", SqlDbType.VarChar, 100),
        new("c_nchar", "nchar(10)", SqlDbType.NChar, 10), new("c_nvarchar", "nvarchar(100)", SqlDbType.NVarChar, 100),
        new("c_binary", "binary(8)", SqlDbType.Binary, 8), new("c_varbinary", "varbinary(100)", SqlDbType.VarBinary, 100),
        new("c_guid", "uniqueidentifier", SqlDbType.UniqueIdentifier), new("c_vmax", "nvarchar(max)", SqlDbType.NVarChar, -1),
    ];

    private static readonly string Ddl = $"CREATE TABLE dbo.typed (id int NOT NULL PRIMARY KEY, {string.Join(", ", Cols.Select(c => $"{c.Name} {c.SqlType} NULL"))});";

    // Each case creates a fresh TDE + CDC database (seconds): "slow generation" is expected here.
    [Fact]
    public void Replica_built_from_events_equals_the_table() =>
        Property.Test(Body, new Settings { TestCases = TestServer.Cases(20), SuppressHealthCheck = HealthCheck.TooSlow });

    private static void Body(TestCase tc)
    {
        var db = Run(() => TestServer.CreateDatabaseAsync(Ddl, "typed"));
        try
        {
            using var m = new Machine(db);
            Stateful.Run(m, tc, new StatefulOptions { StepCount = 12 });
            m.CaptureAndCompare();
        }
        finally { Run(() => TestServer.DropDatabaseAsync(db)); }
    }

    private static T Run<T>(Func<Task<T>> f) => LogMiner.Tests.Blocking.Run(f);
    private static void Run(Func<Task> f) => LogMiner.Tests.Blocking.Run(f);

    private static object DrawValue(TestCase tc, Col c, string label)
    {
        switch (c.Name)
        {
            case "c_tinyint": return (byte)tc.Draw(Generators.Integers().Min(0).Max(255), label);
            case "c_smallint": return (short)tc.Draw(Generators.Integers().Min(short.MinValue).Max(short.MaxValue), label);
            case "c_int": return tc.Draw(Generators.Integers(), label);
            case "c_bigint": return tc.Draw(Generators.Longs(), label);
            case "c_bit" or "c_bit2": return tc.Draw(Generators.Booleans(), label);
            case "c_dec": return new decimal(tc.Draw(Generators.Longs().Min(-999_999_999_999_999_999L).Max(999_999_999_999_999_999L), label)) / 10000m;
            case "c_dec38":
            {
                var mantissa = new BigInteger(tc.Draw(Generators.Longs(), label)) * BigInteger.Pow(10, tc.Draw(Generators.Integers().Min(0).Max(18), label + ".pow"));
                var digits = BigInteger.Abs(mantissa).ToString(CultureInfo.InvariantCulture).PadLeft(11, '0');
                return SqlDecimal.Parse((mantissa.Sign < 0 ? "-" : "") + digits[..^10] + "." + digits[^10..]);
            }
            case "c_num": return new decimal(tc.Draw(Generators.Integers().Min(-99999).Max(99999), label)) / 100m;
            case "c_money": return new decimal(tc.Draw(Generators.Longs(), label)) / 10000m;
            case "c_smallmoney": return new decimal(tc.Draw(Generators.Integers(), label)) / 10000m;
            case "c_float": return tc.Draw(Generators.Doubles().AllowNan(false).AllowInfinity(false), label);
            case "c_real": return (float)tc.Draw(Generators.Doubles().Min(-3.0e38).Max(3.0e38).AllowNan(false), label);
            case "c_date": return DateOnly.FromDayNumber(tc.Draw(Generators.Integers().Min(0).Max(DateOnly.MaxValue.DayNumber), label)).ToDateTime(TimeOnly.MinValue);
            case "c_time": return TimeSpan.FromTicks(tc.Draw(Generators.Longs().Min(0).Max(TimeSpan.TicksPerDay - 1), label));
            case "c_time0": return TimeSpan.FromSeconds(tc.Draw(Generators.Integers().Min(0).Max(86399), label));
            case "c_dt2": return new DateTime(tc.Draw(Generators.Longs().Min(0).Max(DateTime.MaxValue.Ticks), label));
            case "c_dt2_3": return new DateTime(tc.Draw(Generators.Longs().Min(0).Max(DateTime.MaxValue.Ticks / 10000), label) * 10000);
            case "c_dto":
            {
                var offset = TimeSpan.FromMinutes(tc.Draw(Generators.Integers().Min(-840).Max(840), label + ".offset"));
                var utc = new DateTime(tc.Draw(Generators.Longs().Min(TimeSpan.TicksPerDay).Max(DateTime.MaxValue.Ticks - TimeSpan.TicksPerDay), label));
                return new DateTimeOffset(utc + offset, offset);
            }
            case "c_dt": return new SqlDateTime(tc.Draw(Generators.Integers().Min(-53690).Max(2958463), label), tc.Draw(Generators.Integers().Min(0).Max(25919999), label + ".t"));
            case "c_sdt": return new DateTime(1900, 1, 1).AddMinutes(tc.Draw(Generators.Integers().Min(0).Max(65535 * 1440 - 1), label));
            case "c_char": return tc.Draw(Generators.Text().Codepoints(0x20, 0x7E).MaxSize(10), label);
            case "c_varchar": return tc.Draw(Generators.Text().Alphabet("abcXYZ019 ,.-_éüåøÄ€").MaxSize(100), label);
            case "c_nchar": return tc.Draw(Generators.Text().Codepoints(0x20, 0xD7FF).MaxSize(10), label);
            case "c_nvarchar": return tc.Draw(Generators.Text().Codepoints(0x20, 0xD7FF).MaxSize(100), label);
            case "c_binary": return tc.Draw(Generators.Binary().MinSize(8).MaxSize(8), label);
            case "c_varbinary": return tc.Draw(Generators.Binary().MaxSize(100), label);
            case "c_guid": return tc.Draw(Generators.Uuids(), label);
            case "c_vmax": return tc.Draw(Generators.Text().Codepoints(0x20, 0xD7FF).MaxSize(600), label);
            default: throw new InvalidOperationException(c.Name);
        }
    }

    private sealed class Machine : IDisposable
    {
        private readonly string _db;
        private readonly SqlConnection _dml;
        private readonly SqlConnection _reader;
        private readonly MemorySink _sink = new();
        private readonly MemoryCheckpointStore _store = new();
        private readonly EventEncoder _encoder = new(new EncoderOptions { ServerName = "it" });
        private readonly SqlLogReleaser _releaser = new(TestServer.ConnectionString());
        private readonly CaptureSettings _settings = new() { StartPosition = StartPosition.Earliest, MaxRowsPerRead = 50 };
        private readonly Dictionary<int, Dictionary<string, JsonElement>> _replica = [];
        private readonly HashSet<string> _applied = [];
        private readonly int _dbId;
        private readonly string _incarnation;
        private DatabaseCapture _capture;
        private int _nextId = 1;
        private int _appliedCount;
        private int _loads;

        public Machine(string db)
        {
            _db = db;
            _dml = new SqlConnection(TestServer.ConnectionString(db));
            _reader = new SqlConnection(TestServer.ConnectionString());
            Run(() => _dml.OpenAsync());
            Run(() => _reader.OpenAsync());
            (_dbId, _incarnation) = Run(() => TestServer.IdentityAsync(db));
            _capture = new DatabaseCapture(_dbId, db, _incarnation, null, StartPosition.Earliest);
        }

        private List<int> ExistingIds()
        {
            var ids = new List<int>();
            using var cmd = new SqlCommand("SELECT id FROM dbo.typed ORDER BY id", _dml);
            using var r = cmd.ExecuteReader();
            while (r.Read()) ids.Add(r.GetInt32(0));
            return ids;
        }

        private SqlParameter Param(TestCase tc, Col c, string label)
        {
            var p = new SqlParameter("@" + c.Name, c.DbType) { Size = c.Size, Precision = c.Precision, Scale = c.Scale };
            p.Value = tc.Draw(Generators.Integers().Min(0).Max(4), label + ".null") == 0 ? DBNull.Value : DrawValue(tc, c, label);
            return p;
        }

        private void InTransaction(TestCase tc, Action<SqlTransaction> body)
        {
            using var tx = _dml.BeginTransaction();
            body(tx);
            if (tc.Draw(Generators.Integers().Min(0).Max(5), "rollback") == 0) tx.Rollback(); else tx.Commit();
        }

        private void Insert(TestCase tc, SqlTransaction tx, string label)
        {
            var cols = Cols.Where(_ => tc.Draw(Generators.Booleans(), label + ".include")).ToList();
            using var cmd = new SqlCommand($"INSERT dbo.typed (id{string.Concat(cols.Select(c => ", " + c.Name))}) VALUES (@id{string.Concat(cols.Select(c => ", @" + c.Name))})", _dml, tx);
            cmd.Parameters.AddWithValue("@id", _nextId++);
            foreach (var c in cols) cmd.Parameters.Add(Param(tc, c, $"{label}.{c.Name}"));
            cmd.ExecuteNonQuery();
        }

        [Rule(Weight = 3)]
        public void InsertRows(TestCase tc)
        {
            var n = tc.Draw(Generators.Integers().Min(1).Max(3), "rows");
            InTransaction(tc, tx =>
            {
                for (var i = 0; i < n; i++)
                {
                    if (i == 1 && tc.Draw(Generators.Booleans(), "savepoint"))
                    {
                        tx.Save("sp");
                        Insert(tc, tx, $"row{i}");
                        if (tc.Draw(Generators.Booleans(), "rollbackToSavepoint")) tx.Rollback("sp");
                        continue;
                    }
                    Insert(tc, tx, $"row{i}");
                }
            });
        }

        [Rule(Weight = 2)]
        public void UpdateRow(TestCase tc)
        {
            var ids = ExistingIds();
            if (ids.Count == 0) return;
            var id = tc.Draw(Generators.SampledFrom(ids), "id");
            var cols = Cols.Where(_ => tc.Draw(Generators.Booleans(), "set")).Take(8).ToList();
            if (cols.Count == 0) cols.Add(Cols[2]);
            InTransaction(tc, tx =>
            {
                using var cmd = new SqlCommand($"UPDATE dbo.typed SET {string.Join(", ", cols.Select(c => $"{c.Name} = @{c.Name}"))} WHERE id = @id", _dml, tx);
                cmd.Parameters.AddWithValue("@id", id);
                foreach (var c in cols) cmd.Parameters.Add(Param(tc, c, "upd." + c.Name));
                cmd.ExecuteNonQuery();
            });
        }

        [Rule]
        public void DeleteRow(TestCase tc)
        {
            var ids = ExistingIds();
            if (ids.Count == 0) return;
            var id = tc.Draw(Generators.SampledFrom(ids), "id");
            InTransaction(tc, tx =>
            {
                using var cmd = new SqlCommand("DELETE dbo.typed WHERE id = @id", _dml, tx);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.ExecuteNonQuery();
            });
        }

        [Rule]
        public void ChangeKey(TestCase tc)
        {
            var ids = ExistingIds();
            if (ids.Count == 0) return;
            var id = tc.Draw(Generators.SampledFrom(ids), "id");
            InTransaction(tc, tx =>
            {
                using var cmd = new SqlCommand("UPDATE dbo.typed SET id = @n WHERE id = @id", _dml, tx);
                cmd.Parameters.AddWithValue("@id", id);
                cmd.Parameters.AddWithValue("@n", _nextId++);
                cmd.ExecuteNonQuery();
            });
        }

        [Rule(Weight = 3)]
        public void Capture(TestCase tc) => CaptureAndCompare();

        [Rule]
        public void CheckpointReleaseAndBackupLog(TestCase tc)
        {
            CaptureAndCompare();
            Run(() => CheckpointCoordinator.FlushAsync([_capture], _store, _releaser, TimeSpan.Zero, (_, ex) => throw ex, CancellationToken.None));
            Run(() => TestServer.ExecAsync($"BACKUP LOG [{_db}] TO DISK = 'NUL';"));
        }

        /// <summary>
        /// A historical load of the caught-up table: every row exactly once, and each snapshot row byte-identical (as JSON) to the
        /// row's latest stream image - snapshot reads and log-decoded changes must be indistinguishable to consumers.
        /// </summary>
        [Rule]
        public void HistoricalLoad(TestCase tc)
        {
            CaptureAndCompare();
            var chunk = tc.Draw(Generators.Integers().Min(1).Max(4), "chunkSize");
            var layout = _capture.Schema!.Tables.First(l => l.Table == "typed");
            var load = new TableSnapshot(layout, $"h{++_loads}", chunk);
            _capture.RequestSnapshot(load);
            var before = _sink.Snapshot().Count;
            var source = new EngineLogSource(_reader, TestServer.ConnectionString(), CaptureScope.CdcTrackedTables);
            var snapshots = new SqlSnapshotSource(TestServer.ConnectionString());
            for (var i = 0; i < 500 && !load.Done; i++)
                Run(() => _capture.PumpAsync(source, _encoder, _sink, _settings, CancellationToken.None, snapshots));
            Assert.True(load.Done && !load.Failed, $"historical load: done={load.Done} error={load.Error}");
            var reads = new Dictionary<int, JsonElement>();
            foreach (var m in _sink.Snapshot().Skip(before))
            {
                using var doc = JsonDocument.Parse(m.Payload);
                var root = doc.RootElement;
                if (root.GetProperty("op").GetString() != "r") continue;
                var after = root.GetProperty("after").Clone();
                Assert.True(reads.TryAdd(after.GetProperty("id").GetInt32(), after), $"row {after.GetProperty("id")} loaded twice");
            }
            Assert.Equal(_replica.Keys.Order(), reads.Keys.Order());
            foreach (var (id, after) in reads)
                foreach (var (name, streamValue) in _replica[id])
                {
                    var loaded = after.GetProperty(name).GetRawText();
                    Assert.True(streamValue.GetRawText() == loaded, $"row {id} column {name}: stream={streamValue.GetRawText()} snapshot={loaded}");
                }
            CaptureAndCompare();
        }

        [Rule]
        public void RestartFromCheckpoint(TestCase tc)
        {
            Run(() => CheckpointCoordinator.FlushAsync([_capture], _store, null, TimeSpan.Zero, null, CancellationToken.None));
            _capture = new DatabaseCapture(_dbId, _db, _incarnation, Run(() => _store.LoadAsync(_db, CancellationToken.None)), StartPosition.Earliest);
        }

        public void CaptureAndCompare()
        {
            var source = new EngineLogSource(_reader, TestServer.ConnectionString(), CaptureScope.CdcTrackedTables);
            while (Run(() => _capture.PumpAsync(source, _encoder, _sink, _settings, CancellationToken.None))) { }
            var messages = _sink.Snapshot();
            for (; _appliedCount < messages.Count; _appliedCount++)
            {
                var m = messages[_appliedCount];
                if (!_applied.Add(m.MessageId)) continue;
                using var doc = JsonDocument.Parse(m.Payload);
                var root = doc.RootElement.Clone();
                var op = root.GetProperty("op").GetString();
                var image = root.GetProperty(op == "d" ? "before" : "after");
                var id = image.GetProperty("id").GetInt32();
                if (op == "d") _replica.Remove(id);
                else _replica[id] = image.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.Clone());
            }
            CompareWithTable();
        }

        private void CompareWithTable()
        {
            var table = new Dictionary<int, object?[]>();
            using (var cmd = new SqlCommand($"SELECT id, {string.Join(", ", Cols.Select(c => c.Name))} FROM dbo.typed", _dml))
            using (var r = cmd.ExecuteReader())
                while (r.Read())
                {
                    var row = new object?[Cols.Length];
                    for (var i = 0; i < Cols.Length; i++) row[i] = r.IsDBNull(i + 1) ? null : Cols[i].DbType == SqlDbType.Decimal ? r.GetSqlDecimal(i + 1) : r.GetValue(i + 1);
                    table[r.GetInt32(0)] = row;
                }
            Assert.Equal(table.Keys.Order(), _replica.Keys.Order());
            foreach (var (id, row) in table)
            {
                var rep = _replica[id];
                for (var i = 0; i < Cols.Length; i++)
                {
                    var c = Cols[i];
                    var expected = Canon(c, row[i]);
                    var actual = Canon(c, rep[c.Name]);
                    Assert.True(expected == actual, $"row {id} column {c.Name} ({c.SqlType}): table={expected} events={actual}");
                }
            }
        }

        private static string NormDecimal(string s)
        {
            if (s.Contains('.', StringComparison.Ordinal)) s = s.TrimEnd('0').TrimEnd('.');
            return s is "-0" or "" ? "0" : s;
        }

        /// <summary>Canonical text for a value read from the table.</summary>
        private static string Canon(Col c, object? v) => v switch
        {
            null => "NULL",
            SqlDecimal d => NormDecimal(d.ToString()),
            decimal m => NormDecimal(m.ToString(CultureInfo.InvariantCulture)),
            double d => d.ToString("R", CultureInfo.InvariantCulture),
            float f => f.ToString("R", CultureInfo.InvariantCulture),
            bool b => b ? "true" : "false",
            byte[] bytes => Convert.ToBase64String(bytes),
            Guid g => g.ToString("D"),
            DateTimeOffset dto => dto.UtcDateTime.Ticks.ToString(CultureInfo.InvariantCulture) + "@" + dto.Offset.TotalMinutes.ToString(CultureInfo.InvariantCulture),
            DateTime dt when c.SqlType == "date" => dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            DateTime dt => dt.Ticks.ToString(CultureInfo.InvariantCulture),
            TimeSpan t => t.Ticks.ToString(CultureInfo.InvariantCulture),
            IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
            _ => v.ToString() ?? "",
        };

        /// <summary>Canonical text for a value from a published JSON event.</summary>
        private static string Canon(Col c, JsonElement e)
        {
            if (e.ValueKind == JsonValueKind.Null) return "NULL";
            if (e.ValueKind == JsonValueKind.Object) return "UNAVAILABLE";
            return c.SqlType switch
            {
                "float" => e.GetDouble().ToString("R", CultureInfo.InvariantCulture),
                "real" => e.GetSingle().ToString("R", CultureInfo.InvariantCulture),
                "bit" => e.GetBoolean() ? "true" : "false",
                _ when c.SqlType.StartsWith("decimal", StringComparison.Ordinal) || c.SqlType.StartsWith("numeric", StringComparison.Ordinal) || c.SqlType.EndsWith("money", StringComparison.Ordinal)
                    => NormDecimal(e.GetString()!),
                "binary(8)" or "varbinary(100)" => e.GetString()!,
                "datetimeoffset(7)" => Canon(c, DateTimeOffset.ParseExact(e.GetString()!, "yyyy-MM-ddTHH:mm:ss.fffffffzzz", CultureInfo.InvariantCulture)),
                "date" => e.GetString()!,
                "datetime2(7)" or "datetime2(3)" or "datetime" or "smalldatetime" => Canon(c, DateTime.ParseExact(e.GetString()!, "yyyy-MM-ddTHH:mm:ss.fffffff", CultureInfo.InvariantCulture)),
                "time(7)" or "time(0)" => Canon(c, TimeSpan.ParseExact(e.GetString()!, "hh\\:mm\\:ss\\.fffffff", CultureInfo.InvariantCulture)),
                _ when e.ValueKind == JsonValueKind.Number => e.GetRawText(),
                _ => e.GetString()!,
            };
        }

        public void Dispose()
        {
            _dml.Dispose();
            _reader.Dispose();
            Run(async () => await _releaser.DisposeAsync());
        }
    }
}
