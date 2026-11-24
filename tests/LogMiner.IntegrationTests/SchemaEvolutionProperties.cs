using Hegel;
using Hegel.Xunit;
using LogMiner.Sinks;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>
/// Random DDL (add, drop, alter columns; rebuilds; re-clustering; heaps; rolled-back DDL) and random single-row changes on the real engine,
/// while the miner lags, restarts, loses its sink, releases and truncates the log at random. Every change must be delivered once, in
/// order, with exactly the columns and values the row had at that moment (journal oracle). Databases vary in the options that change what
/// is logged: row versioning, accelerated database recovery, table organisation.
/// </summary>
public class SchemaEvolutionProperties
{
    private static readonly string[] Shapes =
    [
        "CREATE TABLE dbo.t (c0 int NULL, id int NOT NULL CONSTRAINT pk PRIMARY KEY CLUSTERED, c1 varchar(20) NULL);",
        "CREATE TABLE dbo.t (id int NOT NULL CONSTRAINT pk PRIMARY KEY NONCLUSTERED, c0 int NULL, c1 varchar(20) NULL);",
        "CREATE TABLE dbo.t (c0 int NULL, g int NOT NULL DEFAULT 1, id int NOT NULL, c1 varchar(20) NULL); CREATE CLUSTERED INDEX cx ON dbo.t (g); ALTER TABLE dbo.t ADD CONSTRAINT pk PRIMARY KEY NONCLUSTERED (id);",
    ];

    /// <summary>Type changes that succeed for every value the test writes.</summary>
    private static readonly Dictionary<string, string[]> Transitions = new()
    {
        ["int"] = ["bigint", "decimal(19,2)", "varchar(40)"],
        ["bigint"] = ["decimal(25,3)", "nvarchar(50)"],
        ["decimal(19,2)"] = ["decimal(25,3)", "varchar(40)"],
        ["decimal(25,3)"] = ["nvarchar(60)"],
        ["bit"] = ["int", "varchar(40)"],
        ["date"] = ["datetime2(3)", "varchar(40)"],
        ["datetime2(3)"] = ["datetime2(7)", "nvarchar(60)"],
        ["varchar(20)"] = ["varchar(40)", "nvarchar(50)", "varchar(max)", "varchar(40) COLLATE Cyrillic_General_CI_AS"],
        ["varchar(40)"] = ["nvarchar(60)", "varchar(max)", "varchar(40) COLLATE Greek_CI_AS"],
        ["varchar(40) COLLATE Cyrillic_General_CI_AS"] = ["varchar(40) COLLATE Greek_CI_AS", "nvarchar(50)"],
        ["nvarchar(50)"] = ["nvarchar(60)", "nvarchar(max)"],
        ["uniqueidentifier"] = ["varchar(40)", "nvarchar(50)"],
        ["char(5)"] = ["char(9)", "varchar(40)"],
        ["varbinary(9)"] = ["varbinary(30)", "varbinary(max)"],
    };

    private static readonly string[] NewTypes = ["int", "bigint", "decimal(19,2)", "bit", "date", "datetime2(3)", "varchar(20)", "nvarchar(50)", "uniqueidentifier", "char(5)", "varbinary(9)"];

    private static string ValueSql(string type, int n) => type.Split('(', ' ')[0] switch
    {
        "bit" => $"{n % 2}",
        "int" or "bigint" => $"{n * 37 - 500}",
        "decimal" => $"{n * 37 - 500}.5",
        "date" => $"DATEADD(day, {n}, CONVERT(date, '2020-01-01'))",
        "datetime2" => $"DATEADD(millisecond, {n * 977}, CONVERT(datetime2(3), '2020-01-01'))",
        "uniqueidentifier" => $"CONVERT(uniqueidentifier, '00112233-4455-6677-8899-{n:D12}')",
        "char" => $"'{(char)('a' + n % 26)}x'",
        "varbinary" => $"CONVERT(varbinary(4), {n})",
        "nvarchar" => $"N'{(char)('а' + n % 32)}-{n}'",
        _ => $"'v{n}'",
    };

    [Fact]
    public void Every_change_is_delivered_with_the_layout_it_was_written_with() =>
        Property.Test(Body, new Settings { TestCases = TestServer.Cases(24), SuppressHealthCheck = HealthCheck.TooSlow });

    private static void Body(TestCase tc)
    {
        var shape = tc.Draw(Generators.SampledFrom(Shapes), "shape");
        var options = "";
        if (tc.Draw(Generators.Booleans(), "readCommittedSnapshot")) options += "ALTER DATABASE CURRENT SET READ_COMMITTED_SNAPSHOT ON WITH ROLLBACK IMMEDIATE; ";
        if (tc.Draw(Generators.Booleans(), "acceleratedDatabaseRecovery")) options += "ALTER DATABASE CURRENT SET ACCELERATED_DATABASE_RECOVERY = ON WITH ROLLBACK IMMEDIATE; ";
        var h = Run(() => CaptureHarness.CreateAsync(shape, ["t"], maxRows: 40, options: options));
        try
        {
            var m = new Machine(h);
            Stateful.Run(m, tc, new StatefulOptions { StepCount = 30 });
            m.CatchUp();
        }
        finally { Run(async () => await h.DisposeAsync()); }
    }

    private static T Run<T>(Func<Task<T>> f) => LogMiner.Tests.Blocking.Run(f);
    private static void Run(Func<Task> f) => LogMiner.Tests.Blocking.Run(f);

    private sealed class Machine(CaptureHarness h)
    {
        private readonly List<(string Name, string Type, bool HasDefault)> _columns = [("c0", "int", false), ("c1", "varchar(20)", false)];
        private readonly List<int> _ids = [];
        private int _nextId = 1, _nextColumn = 2, _n;

        private string Assignments(TestCase tc) =>
            string.Join(", ", _columns.Where(_ => tc.Draw(Generators.Booleans(), "set")).DefaultIfEmpty(_columns[0]).Select(c => $"{c.Name} = {ValueSql(c.Type, ++_n)}"));

        [Rule(Weight = 4)]
        public void Insert(TestCase tc)
        {
            var cols = _columns.Where(c => _required.Contains(c.Name) || tc.Draw(Generators.Booleans(), "include")).ToList();
            var id = _nextId++;
            Run(() => h.DmlAsync("c", id, $"INSERT dbo.t (id{string.Concat(cols.Select(c => ", " + c.Name))}) VALUES ({id}{string.Concat(cols.Select(c => ", " + ValueSql(c.Type, ++_n)))});"));
            _ids.Add(id);
        }

        [Rule(Weight = 4)]
        public void Update(TestCase tc)
        {
            if (_ids.Count == 0) return;
            var id = tc.Draw(Generators.SampledFrom(_ids), "id");
            _n += 100; // always a real change
            var set = Assignments(tc);
            Run(() => h.DmlAsync("u", id, $"UPDATE dbo.t SET {set} WHERE id = {id};"));
        }

        [Rule]
        public void Delete(TestCase tc)
        {
            if (_ids.Count == 0) return;
            var id = tc.Draw(Generators.SampledFrom(_ids), "id");
            Run(() => h.DmlAsync("d", id, $"DELETE dbo.t WHERE id = {id};"));
            _ids.Remove(id);
        }

        [Rule(Weight = 2)]
        public void AddColumn(TestCase tc)
        {
            var type = tc.Draw(Generators.SampledFrom(NewTypes), "type");
            var withDefault = tc.Draw(Generators.Booleans(), "withDefault");
            var name = $"c{_nextColumn++}";
            Run(() => h.DdlAsync(withDefault ? $"ALTER TABLE dbo.t ADD {name} {type} NOT NULL CONSTRAINT df_{name} DEFAULT {ValueSql(type, ++_n)};" : $"ALTER TABLE dbo.t ADD {name} {type} NULL;"));
            _columns.Add((name, type, withDefault));
        }

        [Rule(Weight = 2)]
        public void DropColumn(TestCase tc)
        {
            if (_columns.Count < 2) return;
            var c = tc.Draw(Generators.SampledFrom(_columns), "column");
            Run(() => h.DdlAsync($"ALTER TABLE dbo.t DROP {(c.HasDefault ? $"CONSTRAINT df_{c.Name}, " : "")}COLUMN {c.Name};"));
            _columns.Remove(c);
        }

        [Rule(Weight = 3)]
        public void AlterColumn(TestCase tc)
        {
            var candidates = _columns.Where(c => Transitions.ContainsKey(c.Type)).ToList();
            if (candidates.Count == 0) return;
            var c = tc.Draw(Generators.SampledFrom(candidates), "column");
            var to = tc.Draw(Generators.SampledFrom(Transitions[c.Type]), "to");
            var online = !to.Contains("COLLATE", StringComparison.Ordinal) && tc.Draw(Generators.Booleans(), "online") ? " WITH (ONLINE = ON)" : "";
            Run(() => h.DdlAsync($"{(c.HasDefault ? $"ALTER TABLE dbo.t DROP CONSTRAINT df_{c.Name}; " : "")}ALTER TABLE dbo.t ALTER COLUMN {c.Name} {to} {(c.HasDefault || _required.Contains(c.Name) ? "NOT NULL" : "NULL")}{online};"));
            _columns[_columns.IndexOf(c)] = (c.Name, to, false);
            if (c.HasDefault) _required.Add(c.Name); // still NOT NULL, default gone: inserts must provide it
        }

        private readonly HashSet<string> _required = [];

        [Rule(Weight = 2)]
        public void Rebuild(TestCase tc) => Run(() => h.DdlAsync(tc.Draw(Generators.SampledFrom(new[]
        {
            "ALTER TABLE dbo.t REBUILD;", "ALTER TABLE dbo.t REBUILD WITH (ONLINE = ON);", "ALTER INDEX ALL ON dbo.t REBUILD;", "ALTER INDEX ALL ON dbo.t REORGANIZE;",
        }), "how")));

        /// <summary>Non-unique clustered index on a constant column (uniqueifiers), or back to a heap. Only when the key is not clustered.</summary>
        [Rule]
        public void Recluster(TestCase tc) => Run(() => h.DdlAsync("""
            IF INDEXPROPERTY(OBJECT_ID('dbo.t'), 'pk', 'IsClustered') = 0
            BEGIN
              IF EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID('dbo.t') AND name = 'cx') DROP INDEX cx ON dbo.t;
              ELSE CREATE CLUSTERED INDEX cx ON dbo.t (id);
            END
            """));

        [Rule]
        public void RolledBackWork(TestCase tc)
        {
            // recompile: SQL Server otherwise reuses a cached plan that points at the rolled-back rebuild of an earlier round (error 608)
            var savepoint = tc.Draw(Generators.Booleans(), "savepoint");
            var columns = string.Concat(_columns.Select(c => ", " + c.Name));
            var values = string.Concat(_columns.Select(c => ", " + ValueSql(c.Type, ++_n)));
            var work = $"INSERT dbo.t (id{columns}) VALUES (900000{values}); ALTER TABLE dbo.t ADD ghost int NULL; ALTER TABLE dbo.t REBUILD; DELETE dbo.t WHERE id = 900000 OPTION (RECOMPILE);";
            Run(() => h.SqlAsync(savepoint ? $"BEGIN TRAN; SAVE TRAN sp; {work} ROLLBACK TRAN sp; COMMIT;" : $"BEGIN TRAN; {work} ROLLBACK;"));
        }

        /// <summary>A committed insert around rolled-back DDL in the same transaction.</summary>
        [Rule]
        public void InsertAroundRolledBackDdl(TestCase tc)
        {
            var id = _nextId++;
            var values = string.Concat(_columns.Select(c => ", " + c.Name));
            var exprs = string.Concat(_columns.Select(c => ", " + ValueSql(c.Type, ++_n)));
            Run(() => h.DmlAsync("c", id, $"BEGIN TRAN; INSERT dbo.t (id{values}) VALUES ({id}{exprs}); SAVE TRAN sp; ALTER TABLE dbo.t ADD ghost{_n} int NULL; UPDATE dbo.t SET {_columns[0].Name} = {_columns[0].Name} WHERE id = {id}; ROLLBACK TRAN sp; COMMIT;"));
            _ids.Add(id);
        }

        [Rule(Weight = 4)] public void Pump(TestCase tc) => Run(async () => { await h.PumpOnceAsync(); });
        [Rule] public void Checkpoint(TestCase tc) => Run(() => h.CheckpointAsync());
        [Rule] public void CheckpointReleaseTruncate(TestCase tc) => Run(() => h.CheckpointAsync(release: true));
        [Rule] public void Restart(TestCase tc) => Run(h.RestartAsync);

        [Rule]
        public void SinkOutage(TestCase tc)
        {
            try { Run(async () => { await h.PumpOnceAsync(new DeadSink()); }); }
            catch (IOException ex) { h.Capture.SetFault(ex); }
        }

        [Rule] public void CatchUp(TestCase tc) => CatchUp();

        public void CatchUp()
        {
            Run(h.PumpAllAsync);
            var diffs = h.JournalDiff();
            Assert.True(diffs.Count == 0, string.Join("; ", diffs));
        }

        [Invariant]
        public void Nothing_wrong_was_published(TestCase tc) => Assert.DoesNotContain(h.JournalDiff(), d => !d.Contains(" events for ", StringComparison.Ordinal));
    }

    private sealed class DeadSink : ISink
    {
        public string Name => "dead";
        public SinkHealth Health => new(Name, false);
        public Task StartAsync(CancellationToken ct) => Task.CompletedTask;
        public ValueTask<Task> SendAsync(IReadOnlyList<SinkMessage> messages, CancellationToken ct) => throw new IOException("sink is down");
        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
