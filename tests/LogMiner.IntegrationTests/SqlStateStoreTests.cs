using LogMiner.Pipeline;
using LogMiner.SqlServer;
using Microsoft.Data.SqlClient;
using Xunit;

namespace LogMiner.IntegrationTests;

/// <summary>Position kept in SQL Server: round trip, single active instance (lease), fencing after a lost lease.</summary>
public class SqlStateStoreTests : IAsyncLifetime
{
    private readonly string _db = "lm_state_" + Guid.NewGuid().ToString("N")[..10];
    private string Cs => TestServer.ConnectionString(_db);

    public async Task InitializeAsync() => await TestServer.ExecAsync($"CREATE DATABASE [{_db}];");
    public async Task DisposeAsync() => await TestServer.DropDatabaseAsync(_db);

    private static Checkpoint Cp(string db, uint block) => new(db, "inc", new Lsn(1, block, 1), new Lsn(1, block + 1, 2), new Lsn(1, block, 1), DateTimeOffset.UtcNow, Held: block % 2 == 0, ResumedAt: block % 3 == 0 ? new Lsn(1, block - 1, 4) : default, ResumedInGap: block % 3 == 0,
        Settled: block % 4 == 0, Receipt: block == 20 ? null : $"js|CDC|638684|{block}|srv:db:06E4:{block}:'å",
        Safe: block == 20 ? null : block == 10 ? new SafePosition(new Lsn(1, 2, 1), default, default, false) : new SafePosition(new Lsn(1, block - 4, 1), new Lsn(1, block - 3, 2), new Lsn(1, block - 4, 3), block % 2 == 0));

    [Fact]
    public async Task Checkpoints_round_trip_and_survive_a_new_instance()
    {
        await using (var a = new SqlStateStore(Cs, "srv"))
        {
            await a.EnsureSchemaAsync(default);
            await a.AcquireLeaseAsync(null, default);
            await a.SaveAsync([Cp("tenant_1", 10), Cp("tenant_2", 20), Cp("tenant_3", 10), Cp("tenant_4", 50)], default);
            await a.SaveAsync([Cp("tenant_1", 30)], default);
            await a.SaveAsync([Cp("tenant_4", 20)], default); // nothing of what was there stays
        }
        await using var b = new SqlStateStore(Cs, "srv");
        await b.AcquireLeaseAsync(null, default);
        var t1 = await b.LoadAsync("tenant_1", default);
        var t2 = await b.LoadAsync("tenant_2", default);
        Assert.Equal(Cp("tenant_1", 30) with { UpdatedAt = t1!.UpdatedAt }, t1);
        Assert.Equal(Cp("tenant_2", 20) with { UpdatedAt = t2!.UpdatedAt }, t2);
        var (t3, t4) = (await b.LoadAsync("tenant_3", default), await b.LoadAsync("tenant_4", default));
        Assert.Equal(Cp("tenant_3", 10) with { UpdatedAt = t3!.UpdatedAt }, t3);
        Assert.Equal(Cp("tenant_4", 20) with { UpdatedAt = t4!.UpdatedAt }, t4);
        Assert.Null(await b.LoadAsync("tenant_9", default));
    }

    [Fact]
    public async Task A_table_of_an_earlier_version_is_upgraded_and_its_checkpoints_kept()
    {
        await TestServer.ExecAsync("""
            CREATE TABLE dbo.checkpoints (server_name sysname NOT NULL, database_name sysname NOT NULL, incarnation varchar(80) NOT NULL, restart_lsn binary(10) NOT NULL,
                commit_lsn binary(10) NOT NULL, begin_lsn binary(10) NOT NULL, updated_at datetime2(3) NOT NULL, CONSTRAINT pk_checkpoints PRIMARY KEY (server_name, database_name));
            INSERT dbo.checkpoints VALUES ('srv', 'old', 'inc', 0x00000001000000050001, 0x00000001000000060002, 0x00000001000000050001, SYSUTCDATETIME());
            """, _db);
        for (var i = 0; i < 2; i++) // twice: also when there is nothing to do any more
        {
            await using var s = new SqlStateStore(Cs, "srv");
            await s.EnsureSchemaAsync(default);
            await s.AcquireLeaseAsync(null, default);
            var old = await s.LoadAsync("old", default);
            Assert.Equal(new Checkpoint("old", "inc", new Lsn(1, 5, 1), new Lsn(1, 6, 2), new Lsn(1, 5, 1), old!.UpdatedAt), old);
            await s.SaveAsync([Cp("new", 30)], default);
        }
    }

    [Fact]
    public async Task Only_one_instance_holds_the_lease_and_the_standby_takes_over()
    {
        var active = new SqlStateStore(Cs, "srv");
        await active.EnsureSchemaAsync(default);
        await active.AcquireLeaseAsync(null, default);

        await using var standby = new SqlStateStore(Cs, "srv");
        var waits = 0;
        var takeover = standby.AcquireLeaseAsync(() => Interlocked.Increment(ref waits), default);
        await Task.Delay(2500);
        Assert.False(takeover.IsCompleted);          // blocked while the active instance lives
        Assert.True(waits >= 1);

        await active.DisposeAsync();                 // the active instance dies: its session (and lock) end
        await takeover.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(await standby.CheckLeaseAsync(default));
        await standby.SaveAsync([Cp("tenant_1", 40)], default);
    }

    [Fact]
    public async Task A_fenced_instance_cannot_overwrite_the_new_owners_position()
    {
        var old = new SqlStateStore(Cs, "srv");
        await old.EnsureSchemaAsync(default);
        await old.AcquireLeaseAsync(null, default);
        await old.SaveAsync([Cp("tenant_1", 10)], default);

        // the old instance's session is killed (network blip, failover...): SQL Server releases its lock
        await using (var admin = new SqlConnection(TestServer.ConnectionString()))
        {
            await admin.OpenAsync();
            await using var kill = new SqlCommand($"""
                DECLARE @s int = (SELECT TOP (1) request_session_id FROM sys.dm_tran_locks
                                  WHERE resource_type = 'APPLICATION' AND resource_database_id = DB_ID('{_db}') AND request_owner_type = 'SESSION');
                IF @s IS NOT NULL EXEC ('KILL ' + @s);
                """, admin);
            await kill.ExecuteNonQueryAsync();
        }
        await using var fresh = new SqlStateStore(Cs, "srv");
        await fresh.AcquireLeaseAsync(null, default);
        await fresh.SaveAsync([Cp("tenant_1", 50)], default);

        await Assert.ThrowsAnyAsync<Exception>(() => old.SaveAsync([Cp("tenant_1", 99)], default)); // fenced
        Assert.False(await old.CheckLeaseAsync(default));
        await using var check = new SqlStateStore(Cs, "srv");
        Assert.Equal(new Lsn(1, 51, 2), (await check.LoadAsync("tenant_1", default))!.LastCommitLsn);
        await old.DisposeAsync();
    }
}
