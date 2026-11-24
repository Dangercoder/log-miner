# logminer

Low-footprint, low-latency change data capture for SQL Server (TDE supported) that mines the **transaction log** through the
engine instead of running CDC capture jobs, built for instances with ~1,000+ databases, publishing to pluggable sinks
(NATS JetStream included). Design, measurements and guarantees: [docs/design.md](docs/design.md).

## Layout

| Path | |
|---|---|
| `src/LogMiner.Core` | row decoder, layouts as of an LSN (catalog undo), transaction assembler, capture state machine, checkpoint coordinator, sinks (`ISink`, spool, file, memory) |
| `src/LogMiner.SqlServer` | engine log reader (`fn_full_dblog`), schema loader, activity signals, `sp_repldone`, SQL Server state store + lease, gaps, self-test, capture engine |
| `src/LogMiner.Nats` | NATS JetStream sink |
| `src/LogMiner.Service` | Windows service / console host (`logminer.exe`) |
| `tests/` | Hegel property tests (unit, stateful models, real-engine integration) |
| `tools/LogMiner.Lab` | experiments and benchmarks (`lab service`, `lab e2e`, `lab xe…`, `lab sql <file>`) |
| `infra/` | SQL Server install/config scripts, tenant generator, NATS config |
| `experiments/` | the log-format experiments the design rests on, with outputs |

## Prepare a SQL Server

```sql
EXEC sp_configure 'max text repl size', -1; RECONFIGURE;
-- per captured database: enable CDC on the tables, then drop the CDC jobs (logminer reads the log itself)
EXEC sys.sp_cdc_enable_db;
EXEC sys.sp_cdc_enable_table @source_schema = N'dbo', @source_name = N'orders', @role_name = NULL;
EXEC sys.sp_cdc_drop_job @job_type = N'capture';
EXEC sys.sp_cdc_drop_job @job_type = N'cleanup';
```
Then create the state database: `infra/sqlserver/setup-logminer-db.sql`.

For millisecond latency SQL Server pushes log flushes to the miner through an extended-event session (no Windows privilege, nothing on disk).
The service's login needs `ALTER ANY EVENT SESSION`; without it the miner polls (p99 ~100 ms instead of ~7 ms):

```sql
GRANT VIEW SERVER STATE TO [domain\logminer];
GRANT ALTER ANY EVENT SESSION TO [domain\logminer];
```

## Run

```powershell
dotnet build -c Release
# edit src/LogMiner.Service/appsettings.json (connection, databases, sink), then:
dotnet run --project src/LogMiner.Service -c Release
# or install as a Windows service (it comes back from lost connections, leases and destinations by itself; this is for the process itself):
sc.exe create logminer binPath= "C:\path\to\logminer.exe" start= delayed-auto
sc.exe failure logminer reset= 86400 actions= restart/5000/restart/30000/restart/60000
sc.exe failureflag logminer 1
```

Run two instances (two hosts) for HA, with the same `ServerName`: one holds the lease and captures, the other waits and takes over.

Exit codes: 2 configuration, 4 the self-test failed, 5 a previous run did not come to an end.

On the NATS server use `sync_interval: always` (or R3): with the default, acknowledged messages are lost when its host loses power.

## Verify a SQL Server build

The service verifies every new SQL Server version (after a CU) in a scratch database before it captures anything, and refuses to run if
something it relies on changed. Check a test server before patching production:

```powershell
logminer.exe --self-test "Data Source=testserver;Integrated Security=true;TrustServerCertificate=true"   # exit code 0 = verified, 4 = not
# also says whether the doorbell's push works on that build (if not, the service polls: slower, nothing lost)
```

## Missing changes are never silent

If somebody else releases the log (a CDC capture job, `sp_repldone` by hand, CDC switched off) and it is truncated before it was read, if a
table's CDC is disabled and enabled again, if rows are moved by `ALTER TABLE ... SWITCH`, or if a database is restored, the gap is recorded before the stream goes on, and by default the
affected tables are loaded again (`OnGap`: `Resync` | `Continue`):

```sql
SELECT * FROM logminer.dbo.gaps WHERE acknowledged = 0;
```

Never free a full log with `sp_repldone` while the miner is down: start the miner instead.

If the log says `NO LOG IS RELEASED`, the stream does not hold what it acknowledged (purged, recreated, or the NATS host lost power). SQL Server
keeps everything since the last release. Load the affected databases again, then restart the service.

## Historical loads

Request one with a row in the state database. It runs inside the database's live stream: no table lock, no long
transaction, resumable. Rows arrive as `op: "r"` events, in order with the changes:

```sql
INSERT logminer.dbo.snapshot_requests (server_name, database_name, table_name) VALUES ('sql01', 'tenant_0042', 'dbo.orders');
INSERT logminer.dbo.snapshot_requests (server_name, database_name, table_name) VALUES ('sql01', 'tenant_0042', NULL); -- every captured table
INSERT logminer.dbo.snapshot_requests (server_name, database_name, table_name, chunk_size) VALUES ('sql01', '*', 'dbo.orders', 10000);
SELECT * FROM logminer.dbo.snapshot_requests;  -- status: requested → running → done | partial | failed, rows_emitted, error
```

Tables need a primary key or a unique clustered index. From code: `CaptureEngine.RequestSnapshot(database, table)`.

## Test

```powershell
dotnet test tests/LogMiner.Core.Tests            # properties + stateful models (no server needed)
$env:LOGMINER_SOAK = 10                          # ten times as many random histories against the real engine
dotnet test tests/LogMiner.IntegrationTests      # needs the local SQL Server with the LogMinerTdeCert certificate, and nats-server (LOGMINER_NATS_SERVER)
dotnet run --project tools/LogMiner.Lab -c Release -- e2e --dbs 1200 --tps 200 --seconds 40   # latency benchmark (NATS on :4222)
```
