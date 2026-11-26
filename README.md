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
| `src/LogMiner.Verify` | compares the stream with the tables (`logminer-verify`) |
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

On the NATS server set `sync_interval` well below `ReleaseIntervalMs` (`1s`; the default of 2m is too long) or use R3: what was acknowledged
since the last sync is lost when its host loses power. The miner notices that and publishes it again from SQL Server's log, which has it for
`ReleaseIntervalMs`.

### In Kubernetes

```bash
docker build -t logminer .            # 223 MB, not root, read-only file system
kubectl apply -f infra/k8s/logminer.yaml   # edit it first: connection (a Secret), databases, NATS
```

Two pods per instance: one captures, the other takes over. No volume is needed. What differs from running on the database host (latency, the rights of the
login, failover times) is in [docs/design.md](docs/design.md) under *Off the database host*. Probes and metrics are on port 8080 (`LogMiner__HealthPort`).

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

If the log says `NO LOG IS RELEASED`, the stream does not hold what it acknowledged (purged, recreated, or the NATS host lost power). What the stream
*lost* is published again by itself at the next start or reconnect. What was *purged or deleted* on purpose is not: SQL Server keeps everything
since the last release; load the affected databases again, then restart the service.

## Does the stream match the tables?

```powershell
# request a historical load (next section), and when it is done:
dotnet run --project src/LogMiner.Verify -c Release -- --stream LOGMINER --url nats://127.0.0.1:4222   # exit code 0 = nothing differs, 1 = differences
```

It reads the stream only (no SQL Server, no acknowledgements) and checks every change against the row before it, and every load against what the
stream has built. `--follow` keeps watching, `--strict` also fails for tables that no load was compared with yet.

## Historical loads

Request one with a row in the state database. It runs inside the database's live stream: no table lock, no long
transaction, resumable. Rows arrive as `op: "r"` events, in order with the changes, between two `op: "l"` markers (a consumer that does not need them passes over
them; what it holds and a whole load did not mention is gone from the table):

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
