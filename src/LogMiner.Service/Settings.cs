using LogMiner.Capture;
using LogMiner.Sinks;
using LogMiner.SqlServer;

namespace LogMiner.Service;

/// <summary>Root configuration section "LogMiner".</summary>
public sealed class LogMinerSettings
{
    /// <summary>Logical name of this SQL Server in events and subjects (defaults to the machine name).</summary>
    public string ServerName { get; set; } = Environment.MachineName;
    /// <summary>Connection to the instance (master). Shared memory ("lpc:.") is the lowest-latency option on the database host.</summary>
    public string ConnectionString { get; set; } = "Data Source=lpc:.;Initial Catalog=master;Integrated Security=true;TrustServerCertificate=true;Application Name=logminer";
    public CaptureScope Scope { get; set; } = CaptureScope.CdcTrackedTables;
    public StartPosition StartPosition { get; set; } = StartPosition.Auto;
    /// <summary>Database name patterns to capture ('*' and '?' wildcards); empty = every CDC-enabled database.</summary>
    public string[] Databases { get; set; } = [];
    public string[] ExcludeDatabases { get; set; } = [];
    public int Workers { get; set; } = 8;
    public int MaxRowsPerRead { get; set; } = 20_000;
    /// <summary>Row images of uncommitted transactions buffered per database; a transaction that needs more stops its database's stream.</summary>
    public int MaxBufferedMb { get; set; } = 1024;
    public int CheckpointIntervalMs { get; set; } = 1000;
    /// <remarks>Also how long SQL Server keeps a change after its checkpoint: longer than the sink may take to have an acknowledged event on disk
    /// (JetStream's sync_interval).</remarks>
    public int ReleaseIntervalMs { get; set; } = 10_000;
    public bool ReleaseLog { get; set; } = true;
    /// <summary>Verification of this SQL Server build in a scratch database, once per server version and miner build (see LogMiner.SqlServer.SelfTest).</summary>
    public SelfTestMode SelfTest { get; set; } = SelfTestMode.Require;
    /// <summary>What to do when captured changes may be missing (someone else released the log, the database was restored): load the database
    /// again, or just go on. Always recorded in logminer.dbo.gaps.</summary>
    public GapPolicy OnGap { get; set; } = GapPolicy.Resync;
    /// <summary>Log a database may hold for the newest captured transaction (kept unreleased as a bookmark) before that is released too. The
    /// worst case is this times the number of databases, if all of them are busy with tables that are not captured.</summary>
    public int MaxHeldLogMb { get; set; } = 64;
    public int SweepIntervalSeconds { get; set; } = 30;
    public StateSettings State { get; set; } = new();
    public SignalSettings Signal { get; set; } = new();
    public RoutingSettings Routing { get; set; } = new();
    public SinkSettings Sink { get; set; } = new();
    public int HealthLogIntervalSeconds { get; set; } = 30;
}

public enum SelfTestMode
{
    /// <summary>Do not capture on a server build that was not verified.</summary>
    Require,
    /// <summary>Verify, report loudly, capture anyway.</summary>
    Warn,
    Off,
}

public enum StateStoreType
{
    /// <summary>Checkpoints in a small database on the captured instance; exclusive lease for active/standby HA (default).</summary>
    SqlServer,
    /// <summary>Checkpoints in a local file (host-local mode, pairs with a spool that acknowledges on its own fsync).</summary>
    File,
}

public sealed class StateSettings
{
    public StateStoreType Type { get; set; } = StateStoreType.SqlServer;
    /// <summary>State database on the captured instance (created by infra/sqlserver/setup-logminer-db.sql).</summary>
    public string Database { get; set; } = "logminer";
    public string File { get; set; } = Path.Combine(AppContext.BaseDirectory, "state", "checkpoints.lmc");
}

public sealed class SignalSettings
{
    /// <summary>Let SQL Server push log flushes through an extended-event session (needs ALTER ANY EVENT SESSION). Off, or while it does not
    /// work: poll.</summary>
    public bool Push { get; set; } = true;
    /// <summary>How often pushed events are collected (~55 µs of SQL Server CPU each time, whatever the number of databases).</summary>
    public int DispatchIntervalMs { get; set; } = 4;
    /// <summary>Name of the event session, server-wide (empty: logminer_doorbell_ and <see cref="LogMinerSettings.ServerName"/>). Two miners on
    /// one instance must not share it: each would replace the other's.</summary>
    public string EventSession { get; set; } = "";
    /// <summary>While pushing: poll of every database's log writes that checks the push.</summary>
    public int AuditIntervalMs { get; set; } = 1000;
    /// <summary>While polling: every database's log writes (~2 µs of SQL Server CPU per log file per poll).</summary>
    public int FullPollIntervalMs { get; set; } = 100;
    /// <summary>While polling: databases that wrote within <see cref="HotWindowSeconds"/> (cheap: only their log files).</summary>
    public int HotPollIntervalMs { get; set; } = 5;
    public int HotWindowSeconds { get; set; } = 10;
}

public sealed class RoutingSettings
{
    public string DestinationTemplate { get; set; } = "cdc.{server}.{db}.{schema}.{table}";
    public PartitionKeyMode PartitionKey { get; set; } = PartitionKeyMode.Database;
}

public sealed class SinkSettings
{
    /// <summary>Registered sink provider type: "nats-jetstream", "file", "null", or one added by a plugin.</summary>
    public string Type { get; set; } = "nats-jetstream";
    public SpoolSettings Spool { get; set; } = new();
}

public sealed class SpoolSettings
{
    public bool Enabled { get; set; }
    public string Directory { get; set; } = Path.Combine(AppContext.BaseDirectory, "state", "spool");
    /// <summary>Delivered: the SQL log is released only behind remote acknowledgements (required with State=SqlServer).
    /// Spooled: released behind the local fsync - only with State=File on the same durable volume.</summary>
    public SpoolAck AckOn { get; set; } = SpoolAck.Delivered;
    public long MaxBytes { get; set; } = 20L * 1024 * 1024 * 1024;
    public long SegmentBytes { get; set; } = 64L * 1024 * 1024;
}
