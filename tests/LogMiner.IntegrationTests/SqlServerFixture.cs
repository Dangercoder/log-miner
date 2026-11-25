using Microsoft.Data.SqlClient;

namespace LogMiner.IntegrationTests;

/// <summary>Connection to the local test instance. Tests are skipped when LOGMINER_IT_CS is unset and the default instance is unreachable.</summary>
public static class TestServer
{
    /// <summary>Test cases of a property: LOGMINER_SOAK=10 runs ten times as many.</summary>
    public static int Cases(int normal) =>
        int.TryParse(Environment.GetEnvironmentVariable("LOGMINER_SOAK"), out var factor) && factor > 1 ? normal * factor : normal;

    public static string ConnectionString(string db = "master") =>
        Environment.GetEnvironmentVariable("LOGMINER_IT_CS") is { Length: > 0 } other ? new SqlConnectionStringBuilder(other) { InitialCatalog = db }.ConnectionString
        : $"Data Source=localhost;Initial Catalog={db};Integrated Security=true;TrustServerCertificate=true;Application Name=logminer-tests;Pooling=true";

    public static async Task ExecAsync(string sql, string db = "master")
    {
        await using var conn = new SqlConnection(ConnectionString(db));
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn) { CommandTimeout = 120 };
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Creates a TDE-encrypted, FULL-recovery database with CDC enabled on the given tables and the CDC jobs dropped.</summary>
    public static async Task<string> CreateDatabaseAsync(string tablesDdl, params string[] cdcTables)
    {
        var db = "lm_it_" + Guid.NewGuid().ToString("N")[..12];
        await ExecAsync($"CREATE DATABASE [{db}]; ALTER DATABASE [{db}] SET RECOVERY FULL;");
        await ExecAsync("CREATE DATABASE ENCRYPTION KEY WITH ALGORITHM = AES_256 ENCRYPTION BY SERVER CERTIFICATE LogMinerTdeCert;", db);
        await ExecAsync(tablesDdl, db);
        var enable = string.Join("\n", cdcTables.Select(t => $"EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'{t}', @role_name = NULL;"));
        await RetryOnDeadlockAsync(() => ExecAsync($"EXEC sys.sp_cdc_enable_db; {enable}", db));
        // msdb job metadata deadlocks when databases are set up concurrently: retry the victim
        await RetryOnDeadlockAsync(() => ExecAsync("IF EXISTS (SELECT 1 FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N'capture') EXEC sys.sp_cdc_drop_job @job_type = N'capture';", db));
        await RetryOnDeadlockAsync(() => ExecAsync("IF EXISTS (SELECT 1 FROM msdb.dbo.cdc_jobs WHERE database_id = DB_ID() AND job_type = N'cleanup') EXEC sys.sp_cdc_drop_job @job_type = N'cleanup';", db));
        await ExecAsync($"ALTER DATABASE [{db}] SET ENCRYPTION ON; BACKUP DATABASE [{db}] TO DISK = 'NUL';");
        return db;
    }

    public static async Task RetryOnDeadlockAsync(Func<Task> action)
    {
        for (var attempt = 1; ; attempt++)
        {
            try { await action(); return; }
            catch (SqlException ex) when (attempt < 5 && (ex.Number == 1205 || ex.Message.Contains("deadlocked", StringComparison.Ordinal)))
            {
                await Task.Delay(100 * attempt);
            }
        }
    }

    public static async Task DropDatabaseAsync(string db)
    {
        try { await ExecAsync($"IF DB_ID('{db}') IS NOT NULL BEGIN ALTER DATABASE [{db}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{db}]; END"); }
        catch (SqlException) { }
    }

    public static async Task<(int Id, string Incarnation)> IdentityAsync(string db)
    {
        await using var conn = new SqlConnection(ConnectionString());
        await conn.OpenAsync();
        await using var cmd = new SqlCommand("""
            SELECT d.database_id, CONVERT(char(32), CONVERT(binary(16), r.recovery_fork_guid), 2) + '-' + REPLACE(CONVERT(char(23), d.create_date, 121), ' ', '')
            FROM sys.databases d JOIN sys.database_recovery_status r ON r.database_id = d.database_id WHERE d.name = @db
            """, conn);
        cmd.Parameters.AddWithValue("@db", db);
        await using var r = await cmd.ExecuteReaderAsync();
        await r.ReadAsync();
        return (r.GetInt32(0), r.GetString(1));
    }
}
