using LogMiner.SqlServer;
using Microsoft.Data.SqlClient;
using Xunit;

namespace LogMiner.IntegrationTests;

public class SelfTestTests
{
    [Fact]
    public async Task This_server_is_verified_and_the_scratch_database_is_gone()
    {
        var result = await SelfTest.RunAsync(TestServer.ConnectionString(), CancellationToken.None);
        Assert.True(result.Passed, string.Join("\n", result.Problems));
        Assert.Null(result.DoorbellProblem);
        await using var conn = new SqlConnection(TestServer.ConnectionString());
        await conn.OpenAsync();
        await using var cmd = new SqlCommand($"SELECT DB_ID(N'{SelfTest.DatabaseName}')", conn);
        Assert.Equal(DBNull.Value, await cmd.ExecuteScalarAsync());
    }

    [Fact]
    public async Task A_database_that_is_not_ours_is_never_dropped()
    {
        await TestServer.ExecAsync($"CREATE DATABASE {SelfTest.DatabaseName};");
        try
        {
            var result = await SelfTest.RunAsync(TestServer.ConnectionString(), CancellationToken.None);
            Assert.False(result.Passed);
            Assert.Contains(result.Problems, p => p.Contains("not touching it", StringComparison.Ordinal));
            await TestServer.ExecAsync("CREATE TABLE dbo.still_here (id int);", SelfTest.DatabaseName);
        }
        finally { await TestServer.DropDatabaseAsync(SelfTest.DatabaseName); }
    }

    [Fact]
    public async Task Results_are_remembered_per_version_and_build()
    {
        var db = "lm_state_" + Guid.NewGuid().ToString("N")[..10];
        await TestServer.ExecAsync($"CREATE DATABASE [{db}];");
        try
        {
            var log = new SelfTestLog(TestServer.ConnectionString(db), "srv");
            await log.EnsureSchemaAsync(CancellationToken.None);
            Assert.False(await log.IsVerifiedAsync("16.0.1", CancellationToken.None));
            await log.SaveAsync(new SelfTestResult(false, "16.0.1", ["wrong"], TimeSpan.Zero), CancellationToken.None);
            Assert.False(await log.IsVerifiedAsync("16.0.1", CancellationToken.None));
            await log.SaveAsync(new SelfTestResult(true, "16.0.1", [], TimeSpan.Zero), CancellationToken.None);
            Assert.True(await log.IsVerifiedAsync("16.0.1", CancellationToken.None));
            Assert.False(await log.IsVerifiedAsync("16.0.2", CancellationToken.None));
        }
        finally { await TestServer.DropDatabaseAsync(db); }
    }
}
