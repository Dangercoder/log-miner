# logminer — design

Change data capture for SQL Server 2022 (TDE on) that reads the **transaction log**, not CDC change tables, for one instance
with ~1,200 databases, publishing to a pluggable sink (NATS JetStream first). Claims marked *measured* come from
`experiments/` and `tools/LogMiner.Lab` on SQL Server 2022 CU28 (16.0.4325.4), 1,200 TDE + CDC tenant databases, one 8-core host.

## 1. Reading the log

| Option | Verdict |
|---|---|
| CDC change tables + capture jobs | No: 1,200 Agent jobs, every change written twice, 5 s polling |
| Read `.ldf` files directly | Not possible while SQL Server runs: the file is opened exclusively (*measured*, even elevated with `FileShare.ReadWrite`). TDE also encrypts it. Would need raw volume reads/VSS + TDE key handling |
| **`sys.fn_full_dblog` through the engine** | **Chosen.** SQL Server decrypts TDE; the miner never holds key material. Takes `@dbid`, so one statement (one cached plan) serves every database (*measured*) |

### CDC is *enabled* on tables but never *run*
Enabling CDC on a table changes what SQL Server logs; the capture/cleanup jobs are dropped and change tables stay empty.

| | CDC enabled (no job) | CDC off |
|---|---|---|
| `UPDATE` one int column | full before + after row (*measured*) | 1-byte diff at an offset: the new value is not derivable |
| log retention | held until `sp_repldone` (REPLICATION) | truncated by log backups regardless of the reader |
| `TRUNCATE`, minimally logged bulk load | refused / row-logged (*measured*) | invisible / page images |
| server-side filter | flag bit `0x0004` marks replicated records (*measured*) | filter by allocation unit |

Costs: larger update log records (~2x row width), no `TRUNCATE` on captured tables, `max text repl size = -1` for LOBs
> 64 KB (*measured*, error 7139), one-time enable per table. **A captured database with tables that are not CDC-enabled is
reported loudly** (schema load lists them; engine warns; health counts them) — the benchmark found exactly such a tenant.

### What is read, and how
* Records: insert/delete/modify rows (full images when replicated), commit (`[Transaction Begin]` gives the begin LSN),
  abort, compensation records (flag `0x0001` — **not** flagged replicated; filtering on `0x0004` alone would publish
  rolled-back savepoint work), `LOP_REPL_COMMAND` (DDL → reload layouts). Page splits, rebuilds, ghost cleanup produce no
  replicated row records (*measured*).
* A `@start` older than `log_min_lsn` is **refused, never moved silently** (see *Gaps* in section 3). The function itself *would* move it
  (*measured*, `experiments/31`), and the log can be truncated between the check and the read: so the first record that comes back must be the
  one that was asked for. `@end` is never passed: a non-existent `@end` raises **error 9003 at severity 20** (session killed, "possible
  corruption" in the errorlog) (*measured*). A `@start` beyond the end is refused, too: the database was put back to an earlier state.
* **Only hardened log is read.** The function also returns blocks that are still in memory (*measured*, `experiments/29`): the commit record is
  there while its session still waits for the disk. A crash would un-happen that transaction and use its LSNs again, and the real
  transactions there would be taken for already published. `log_end_lsn` is the end of the hardened log; reads stop below it.
* **The database is identified with every read.** The log is read by id, the catalog and `sp_repldone` go by name, and both move: a dropped or
  detached database's id is taken by the next one at once, names are swapped, a restore puts another history in the same place. Databases made
  from one backup have the same LSNs and rowset ids, so a mix-up would not show (before this, swapping two tenants' names published each
  one's rows as the other's). Id, name and recovery fork are checked under the database's shared lock (~5 µs, `experiments/27`). A database that
  changed ends its capture; whatever has its id and its name now is found by the next refresh (within a second).
* **The database is entered before its log is read** (`DatabaseIdentity.Held`). `fn_full_dblog` reads a database by id from outside, without
  keeping anybody from restoring it or taking it offline. At the wrong moment that call fails an assertion inside SQL Server
  (`logtrunc.cpp:834`, `stLSN != NullLSN`) whose session dies with the `DBTABLE` spinlock: **every session of the instance hangs until it is
  restarted** (*measured*: hung the development instance, `experiments/35`; reproduced on a throwaway one, `experiments/36`, within 4 restores and
  within 4 offline/online cycles, also with an explicit start). The full statement with its checks in front did not (83 restores, 76 offline
  cycles), so the window was small - but it was there. Now a procedure call inside the database holds its shared lock while the read runs below
  it, in master:

  | | hangs | restores refused, one reader | plan cache, 1,200 databases | per read |
  |---|---|---|---|---|
  | read from master (before) | bare call: yes | 30 of 90 | 1.4 MB | 1.86 ms (the same small log, container) |
  | read inside the database | no | 2 of 80 | **1.65 GB** (a plan of 1.4 MB each) | - |
  | **held: entered, read in master** | **no: 548 restores, 297 offline cycles, 319 detach/attach** | **0 of 164** | one plan | 1.84 ms + 0.08 ms for the guard |

  What has to run in the database's own context (layouts, `sp_repldone`) has a plan per database. Those plans are small (the identity is checked by
  one statement in master), large statements that are needed once are compiled for that use (`RECOMPILE`), and `DBCC OPENTRAN` runs from master: with
  1,200 databases the service leaves 54 MB of plans 20 s after a start (306 MB before) and ~5 MB once it runs (*measured*, `experiments/39`).

  Three details carry it (*measured*, `experiments/36`): the procedure's name is in a *variable*, because a name in the text opens the database
  when the batch is compiled, before any guard in it runs; a database in `SINGLE_USER` is left alone (`DATABASEPROPERTYEX` takes no lock), its one
  connection is the administrator's; `DEADLOCK_PRIORITY LOW` makes the miner the victim, not the restore. `sys.database_recovery_status`, read
  from master, made 25 of 25 restores fail in a loop: it is only read under the lock now, and not at all for databases that are already captured.
  `sys.dm_db_log_stats` and `sys.dm_io_virtual_file_stats` neither hang nor refuse anything. What is *not* excluded: a reader that waits for a
  slow sink keeps its database open meanwhile, so a plain `RESTORE` (without `SINGLE_USER ... ROLLBACK IMMEDIATE`) waits for it.
* **Every transaction is proved complete.** The commit record's `[Replicated Records]` is exactly the number of replicated records the
  transaction logged, undone ones too - in every recovery model, with row versioning, accelerated database recovery, bulk loads, LOBs, DDL
  (*measured*, `experiments/28`). A transaction is only published when that many were received. One that is not is read again from its
  beginning; if that is gone it is a recorded gap; anything else stops the stream. Every replicated commit comes out of the assembler, also
  one of which nothing is left: it holds log until it is released (*measured*, `experiments/30`).
* **No replicated record is ignored because it is unknown.** Row records, LOB pages and values and brackets are known (`experiments/28`); any
  other kind, in a captured rowset, stops the stream.
* **Undo.** A compensation record's `[Previous LSN]` is the transaction's record *before* the one it undoes, and undo runs strictly
  backwards, so everything the transaction logged above it is cancelled (*measured*, `experiments/14`). Counting compensation records per
  allocation unit is wrong: DDL and LOB work also undo row changes that were never captured, and cancelled unrelated earlier changes.
* **Progress markers.** The query also returns one payload-free row per log block (slot `0001`). Without them a scan that
  matches nothing cannot advance and every later read re-scans the same log — *measured*: 80–130 ms pumps that saturated the
  workers. Markers reach the assembler as no-op records.
* Incremental read cost: **0.09 ms** (*measured*); a full scan of an 8 MB active log: 34 ms.
* **Changes of the set of tables come from the log, too.** `sys.sysschobjs` is allocation unit 281474978938880 in every database (`experiments/37`): a
  committed change of it (table created, renamed, dropped, CDC switched on or off) makes the layouts and the hold be looked at again. Asking the
  catalog instead cost 10-12 ms per database and minute, a fifth of a core with 1,200 databases.
* Current layouts come from `sys.system_internals_partition_columns` (dropped columns, uniqueifier, metadata-only added columns
  with defaults), loaded on a separate pooled connection (a reload can be needed mid-read). Each load records a log position read
  *before* the catalog; **DDL older than that causes no reload** — *measured*: replaying CDC-enable DDL otherwise
  cost one ~120 ms catalog read per tenant on its first pump under load.
* Decoded: all common types. `xml`, `sql_variant`, legacy LOBs and off-row LOB values are `{"unavailable": true}`;
  compressed and sparse tables are refused explicitly. Engine-mode limit (`fn_dblog` columns are `varbinary(8000)`): rows
  > 8,000 bytes in-row and off-row LOB chunks are truncated → marked unavailable.
* `commit_time` is converted from server-local (as logged) to UTC with the server's time zone rules at that time (in the hour that occurs
  twice, the second).
* Text comes out exactly: nvarchar may hold half a surrogate pair (as a `\ud83d` escape), and char/varchar bytes that are not valid text in
  their code page are `{"undecodable": "<base64>"}`. A value outside its type's range is refused.
* **Changes that are not in the log as rows** cannot be published, so they are reported: `ALTER TABLE ... SWITCH` (its marker and the rowset's new
  owner are in the log) is a gap of the table, with a reload by default; DDL that converts values or gives existing rows a value is a warning
  (`experiments/32`).

### Layouts as of an LSN
The log being read is often older than the catalog: the miner was down, the sink was out, a fault rewound the stream, or a migration
(add column, backfill, alter) ran faster than the miner. An index rebuild gives the table a new rowset, `ALTER COLUMN` a new physical
column; a record must be decoded with the layout of *its own moment*. (Decoding with today's catalog silently dropped everything written
before a rebuild and published NULLs for altered columns — nightly index maintenance during a sink outage was enough.)

* **The catalog's own row changes are in the same log** (`sysrscols`, `sysrowsets`, `syscolpars`, `sysschobjs`, `sysseobjvalues`). Every
  change that matters for decoding is a full-row insert or delete (*measured* for rebuild, online rebuild, compression, type, collation,
  precision, nullability, `(max)`, add, drop: `experiments/09`–`15`). Starting from the current catalog, they are undone newest first, which
  gives the layout of every rowset at every LSN still to be read. No schema history is persisted; any number of DDL statements is handled,
  also inside one transaction with row changes, rolled back, or on tables that have been dropped since.
* In-place catalog changes are found by `hbcolid`, which is the null bit position except after an `ALTER COLUMN` between types of one size; a
  change in several places of one row is one `LOP_MODIFY_COLUMNS` record (`experiments/33`).
* **Row records keep their rowset id** after the rowset is gone, so `(rowset id, LSN)` finds the layout.
* **Nothing is assumed.** The system tables' rows are decoded with their own layouts and column names, read from the catalog. Every logged
  row that should still exist must equal the live catalog and every removed row must be absent; a contradiction is either a DDL that
  committed between the two reads (retried) or a format this version does not understand (fail closed).
* Excluded: transactions that did not commit, work undone by a savepoint rollback, and system transactions (`[Xact Type]` 3: page splits
  move catalog rows).
* **Nothing is skipped.** A committed change that still cannot be attributed to a layout stops its database's stream
  (`UndecodableChangeException`), log retained. The only records ignored are those of tables published by transactional replication.
* During an ONLINE rebuild only the source rowset's records are replicated records (*measured*), so nothing is published twice.
* **Every change comes under the name its table had when it was made**, whether the miner was there or not, also when the table is gone by now: renames and
  schema transfers are changes in place of the table's row in `sysschobjs`, in one place or in several (`experiments/38`; of those only the old lengths
  are logged), and are undone like the rest. A renamed table is a recorded gap (`Renamed`, with a load by default): its rows were published under the
  old name. A schema that is gone has no name any more: `schema#<id>`.

## 2. Which of 1,200 databases changed? (the "doorbell")

| Signal | Cost, 1,209 DBs (*measured*) |
|---|---|
| `sys.dm_io_virtual_file_stats(NULL, …)` | 4.5 ms SQL CPU per poll (~2 µs per log file) |
| `sys.dm_os_performance_counters` (one counter) | ~5.5 ms per poll |
| PDH shared memory (`Repl. Pending Xacts`, live) | 6.3 ms of *our* CPU per poll, needs admin |
| ETW (kernel file writes, or XE → `etw_classic_sync_target`) | needs an elevated consumer (or Performance Log Users); not pursued |
| **XE live stream of `log_flush_complete`, dispatch forced** | **~55 µs SQL CPU per collection, whatever the number of databases; no Windows privilege** |

Polling ties latency to the number of databases: a tenant that has been idle is found by the next poll of everything. **`DoorbellSignal`**
therefore lets SQL Server report the flushes itself, and polls only to check it:

* **Push (`LogFlushStream`).** An extended-event session holds nothing but `log_flush_complete` (database id, log block, bytes written). Its
  live stream is read over an ordinary connection (`sys.fn_MSxe_read_event_stream`, what SSMS uses for "watch live data"). By itself the
  stream arrives up to 1 s late (`MAX_DISPATCH_LATENCY` cannot be lower). **Reading `sys.dm_xe_session_targets` makes the engine dispatch at
  once**, so that is done every `Signal.DispatchIntervalMs` (4 ms): 55 µs of CPU when filtered to the session's address, 28 ms when not,
  because that renders `system_health`'s ring buffer (`experiments/21`, `22`). This is a push in two steps, not an interrupt: on average half
  a dispatch interval is added.
* **Visible when pushed.** On Windows none of 6,000 pushed blocks was missing from a `fn_full_dblog` read started at once, under concurrent commits
  (`lab xerace`). SQL Server on *Linux* announces a block before it is on disk: 8 % were not readable yet, for as long as the disk takes (over 63 ms,
  seconds under load). A read that does not reach the pushed block is repeated (up to 40 times, 8 s); giving up after 60 ms, as before, left such
  events to the next sweep: **1-10 s late**. The health line counts both (`early reads repeated`, `given up`).
* **Audit (`LogWriteAudit`).** Once a second all log files' `num_of_bytes_written` are polled and compared with the sum of the pushed
  `write_size`. They agree to the byte, except for the log file's own headers (12 or 24 KB at a VLF change, *measured*). A database whose
  unexplained bytes grew is read; a push that fell silent while log is written is restarted after three polls.
* **Fallback.** Until the push is attached, and whenever it is refused, drops out or is not understood, everything is polled as before: all
  databases every 100 ms, those active within 10 s every 5 ms. Logged as an error, shown in the health line, retried with back-off.
* **Nothing depends on it but latency.** A sweep re-reads every database once in 30 s anyway, spread out (all at once pushed p99 from ~130 ms
  to ~420 ms, *measured*). A poll that fails takes the whole signal down, loudly, and the engine restarts it (a type-cast bug once left only
  the sweep running: p50 10.8 s).
* **The engine never waits for the miner.** The session may lose events (`ALLOW_SINGLE_EVENT_LOSS`); with a reader that is attached but
  stuck, writers go on (tested).

**Undocumented, so distrusted.** The stream's framing (`LogFlushDecoder`) and the forced dispatch are not documented. Every length, id and
version is checked and anything unexpected means polling; field offsets come from the documented `sys.dm_xe_object_columns`, so an added
column is not a problem. The self-test reports whether the push works on a build, but does not fail for it. Microsoft's own parser (XELite)
was not used: its licence has to be accepted and it depends on an older SqlClient.

**What it costs others.** Whole-server CPU above a load without any doorbell: +2.8 % of one core (poll: +6 %) at 200 and at 2,000 tx/s; at
48,000 tx/s +2 µs per commit and +4 µs commit p50 (`experiments/26`). One side effect: the forced dispatch is server-wide
(`experiments/24`). Sessions with in-memory targets do not care. A trace that writes a *file* writes what it has collected every few
milliseconds instead of when its buffer is full: small writes, 9 % more file bytes at 2,000 events/s, no measurable CPU (`experiments/24b`).
SQL Server Audit is built on event sessions too and should behave the same, but that was **not measured**. The service names such sessions
and audits at start; `DispatchIntervalMs` or `Push: false` are the knobs.

## 3. Durability — chain of custody

```
SQL Server log ──read──> DatabaseCapture ──SendAsync──> [SpoolSink] ──> ISink (JetStream / Kafka / …)
   ▲ held until               │ one unacked call per DB      │ optional local store-and-forward
   │ sp_repldone              ▼                              ▼
   └── release ◄── durable checkpoint (SQL Server, fenced) ◄── acknowledged prefix
```

1. The **SQL Server log** keeps every record of a CDC table until `sp_repldone` (replication hold).
2. **DatabaseCapture** publishes in commit order with **at most one unacknowledged sink call per database** (all ready
   transactions batched into it), retiring acknowledgements in order.
3. **Checkpoint** `{restart LSN, last acknowledged commit, begin LSN, incarnation}`. The restart LSN is at or before the
   first record of every transaction that is open, *being prepared* (decoding may take a catalog read; a checkpoint taken just then used to
   skip it for good), or not acknowledged. **Correctness does not rest on it:** reading resumes at or before the last acknowledged commit
   (which the bookmark keeps), every later commit says by itself where it began and how many records it has, and a model property moves the
   restart LSN anywhere later without losing anything.
4. **`sp_repldone`** is only called behind *durable* checkpoints, and normally never for the newest acknowledged transaction (next section) -
   and only after the destination was **audited**: the stream is still the one that was published to, and holds the newest acknowledged message
   of every database that is about to be released, under its id. Releasing is the point of no return; until then everything can be
   published again. In a database that is published for replication the miner does not release at all.
   **An acknowledgement is not yet a disk.** Once per `ReleaseIntervalMs` the miner looks: the position that was durable at the last look is an
   interval old and audited now; it becomes the **safe position**, and what was releasable then is released once that safe position is itself in
   the checkpoint. With every checkpoint goes the destination's **receipt** for the newest message (stream, its creation, sequence, id). Before
   anything is sent again - at the start, and when a connection comes back - the destination is shown its receipts. One that does not have that
   message any more lost what it had acknowledged (a NATS host without power, between two syncs): the stream goes back to the safe position, whose
   log SQL Server still has, and **publishes it again by itself**; loads that had ended shortly before are requested again (a load counts as done when
   the stored receipt covers its end). Another stream than the
   one the receipt is of is a recorded gap and goes back, too. A stream that has no safe position yet goes back to where it began.
5. Any failure rewinds that database to its durable checkpoint (never forward); the log is retained meanwhile. What committed before a
   transaction that cannot be decoded goes out first; it and everything behind it waits, however often it is tried.
6. **The durable position never moves back** (a model invariant): not while the log is re-read after a restart, and not when the sink fails
   while a newer checkpoint is being saved.

### Gaps: log that disappears before it was read
SQL Server keeps replicated log until it is released, but not only the miner can release it: a CDC capture job or Log Reader Agent that
comes back, `sp_repldone @reset = 1` run because the log filled up while the miner was down, CDC being disabled. Reading then used to go on
silently at the oldest record.

* **Bookmark.** The newest acknowledged transaction is held back (`checkpoint.held`). While it is SQL Server's oldest unreleased transaction
  (`DBCC OPENTRAN`, persistent, no log reader role needed), nobody else released log beyond the miner's position.
* **Detected when it matters.** A position that is no longer in the log is refused by the reader. If the bookmark is intact, nothing is
  missing and reading goes on; if it is gone (`Broken`) or there was none (`Unknown`), the gap is written to `logminer.dbo.gaps` *before* the
  stream moves on. `OnGap`: `Resync` (default: a historical load of the database is requested in the same transaction; deletes inside the gap
  are found by its markers, section 4) or `Continue`. A transaction that began before the point where reading went on is never published in part, also after a
  restart (the point is part of the checkpoint: `resumed_at`); if the truncation looked harmless, that contradiction is itself recorded as a
  gap.
* **Reported while there is still time.** Every minute the bookmarks are checked (*measured*: ~0.1 % of one core for 1,200 databases); a
  foreign release is logged as an error and counted in health.
* **Bounded.** When a caught-up stream's bookmark holds more than `MaxHeldLogMb` (64; worst case that times the number of databases) because
  other tables are busy, it is released too (recorded in the checkpoint first); until the next captured change a truncation beyond the
  position is then reported as `Unknown`.
* **One table.** CDC disabled and enabled again on a table (e.g. to change its column list) leaves no trace of the changes in between. It is
  noticed from `cdc.change_tables.start_lsn`: a table none of whose capture instances was there before was not captured continuously
  (creating a second instance before dropping the first is). A table that is captured later than the database's stream is reported the same
  way (`NewTable`), and with `Resync` the rows it already had are loaded.
* A restored or re-created database (new incarnation) is recorded as a gap as well: its contents no longer follow from the published events.
* CDC refuses databases with delayed durability (error 22891).
* **Positions inside a block.** The oldest record is exact to the slot, a position is the last record that was read. If the log begins inside the block of
  a position, nothing is missing as long as that block had been read to its end: no block is ever added to once a read up to `log_end_lsn` has
  returned it (*measured*, `experiments/40`). The checkpoint says whether that is so (`settled`); any other position in such a block is refused like
  one that is gone altogether. (Before, transactions behind such a position were skipped without a word; found by a review.)

### Where the position lives (default: SQL Server)
`SqlStateStore` keeps checkpoints in a small `logminer` database on the instance: the service is stateless (reinstall or
move it freely), and the data sits on storage that is already backed up and monitored.
* **Lease / HA:** an exclusive session app lock (`sp_getapplock`) on an **unpooled** connection (a pooled one keeps the
  session — and the lock — alive after dispose; found by the takeover test). Standby instances wait and take over in seconds.
* **Fencing:** every save runs on the lease session and verifies the lock inside the batch; a fenced instance cannot overwrite
  a newer owner's position (tested by killing the lease session).
* **Durable before release:** saves commit with `DELAYED_DURABILITY = OFF`.
* **Second record of "where we are":** each tenant database keeps its own `sp_repldone` position. With no usable checkpoint,
  `StartPosition.Auto` resumes from SQL Server's oldest unreleased replicated transaction (`DBCC OPENTRAN`
  `REPL_NONDIST_OLD_LSN`), so a lost checkpoint never skips anything.
* **Incarnations:** a restored / re-created tenant rewinds its LSNs. Checkpoints, message ids and dedupe keys carry the
  incarnation (`recovery_fork_guid`, new after every restore and snapshot revert, + `create_date`); a foreign checkpoint is discarded with a
  warning and a recorded gap.
* Alternatives: `FileCheckpointStore` (host-local; atomic temp + fsync + rename, previous generation kept), or any
  `ICheckpointStore` (an object store with ETag CAS fits).

### Starting without a checkpoint
A `Latest` start whose log was truncated before its first read (released by someone else, or ordinarily: nothing holds it yet) is a recorded gap: the log has no holes, so the first
record that is read must reach back to the durable start point (found by the pipeline model; before, those changes were lost without a word).
`Earliest` replays the retained log. `Latest` silently skips the existing log, then goes live, but it **publishes nothing until
that start point is a durable checkpoint**. A `Latest` start point depends on *when* the catch-up ended. If the process crashed
before saving it, the restart would choose a different one. The result would be a gap after events already delivered, or
older history delivered after newer events, which leaves a deduplicating consumer with stale rows. The model test found
both. No checkpoint is written during the catch-up. Holding for the first checkpoint costs at most one checkpoint interval, once per database.
(*Measured*: `sp_repldone` naming a transaction that is already distributed is a no-op, so releasing at the start point is
harmless — `experiments/08`.)

### Sinks
`ISink` is the extension point (NATS JetStream, file, null; Kafka/Event Hubs/HTTP plug in the same way):
* **Ack** = durably accepted by the destination. **Duplicates**: at-least-once with a deterministic message id
  (`server:db:incarnation:commitLsn:seq`, names escaped, ASCII) for destination de-duplication. **Consumers should keep a high-water mark per
  `(source.db, source.incarnation)` of `(commit_lsn, seq)`** and pass over anything at or below it: a replay that is partly older than the
  destination's duplicate window is stored again in part, behind newer events. `seq` ascends within a transaction (not contiguous when it also
  changed replicated rows that are not captured). Rows of a historical load have an id of their own for every reading of a chunk, and no
  `commit_lsn` and `seq`: they have no place in the commit order and neither move that mark nor are compared with it (with them, a consumer dropped a
  change that came right behind a load; found by a review). The mark is one per database only with the default key: with `PartitionKey` `Table` or
  `RowKey` the messages of one transaction may be stored in another order, so it has to be kept per key (the service warns).
* **No overtaking per key** (key = database by default). A failure inside a sink cannot stop a later call the caller already
  made, so **callers keep one unacknowledged call per key** (Kafka needs broker sequence numbers for the same reason); within
  a call the sink stores a key's messages in order and stores nothing after a failed one. The JetStream sink sends one
  message per key at a time (lanes), pipelined across keys. **Nothing is sent late:** a connection on which anything went unanswered is given
  up with all that is in it and a new one is opened (the client would send it when it is connected again, behind newer messages). Only a
  PubAck of the configured stream counts. An existing stream is never changed. Key `RowKey` orders by row only; a changed key is two rows.
* **Throughput of one database** is one round trip per message (one unacknowledged message per lane): a transaction of 4,096 rows takes ~0.25 s on the lab
  host. Databases do not slow each other down.
* **A transaction is buffered until it commits.** One whose rows exceed `MaxBufferedMb` (1,024) stops its database, log retained. Nothing is spilled to
  disk: raise the limit and restart, or give that part up (section 8).
* **An event that is too large** for the destination comes with its largest values marked as unavailable (never the key) instead of stopping
  its database.
* **SpoolSink** (optional): CRC-32C-framed segments, group-commit fsync, forwarding from memory right after the write,
  go-back on failure, one batch in flight, deletion only behind a persisted cursor, torn-tail truncation and fail-closed
  recovery (a damaged record that had been on disk, a missing segment: it refuses to start); it forwards strictly in order, so a message the destination
  refuses for good holds up every database behind a spool (without one, only its own), **exact message-id de-duplication** (a position-based high-water mark was replaced: the model test showed it could
  drop a message that was never durably spooled). Ack modes: `Delivered` (default; required when the checkpoint is in SQL
  Server; what an earlier run left in the spool is dropped, the source sends it again) or `Spooled` (only with `State: File`, on the same
  durable volume; disabling a spool that holds undelivered records is refused).

## 4. Historical loads (snapshots)

Watermark chunking, as in Netflix DBLog, run inside each database's ordered stream. No table lock, no long transaction, no
second pipeline:

1. One round trip reads the log end **L**, then `TOP (n)` rows after the last key `WITH (READCOMMITTEDLOCK)`, then the log
   end again (**H**).
2. While the stream decodes transactions committed in **[L, H)**, chunk rows whose key they touch are dropped. The stream
   carries a newer image of those rows.
3. When the stream reaches H (the first transaction committed at or after H, or the end of the log), the remaining rows are
   emitted as `op: "r"` events in stream order.

A load is announced and concluded in the stream, on the table's subject, by events `op: "l"` with no key and no images:
`"load": {"id", "phase": "begin" | "end", "rows"}`. The begin comes before the first chunk is read, so **every row the table has at the end was
loaded or changed between the markers**: what a consumer holds besides does not exist any more (deletes that were lost in a gap). `rows` counts the
loaded rows; when fewer different keys arrived, the load is not whole and says nothing. A chunk whose acknowledgement was lost is read again, also
behind the end. A load that starts over (restart, fault) begins again with the same id. Consumers ignore operations they do not know.

**Why it is exact:** a row read at time *t* reflects every commit completed before *t*. A key with no commit in [L, H) has the
same value at H. A key with a commit in [L, H) is superseded by that commit's event. Locking read committed is forced because
an RCSI version can lag a commit whose log record is already written. Fully durable commits are required as well.

* **Resume and failure:** the resume key advances only after the sink acknowledges a chunk's rows. Any fault re-reads the
  chunk, and snapshot rows are idempotent upserts. Snapshot rows never move the stream checkpoint.
* **Errors:** a failing chunk read (lock timeout, dropped table) never faults the stream. The load retries with backoff and
  fails on its own after 5 consecutive failures (about half an hour of what the server reports: locks, deadlocks, maintenance). A load that
  did not deliver everything is an error in the log, not information.
* **Keys:** of any type, also nullable (unique clustered index), in any code page, with half surrogate pairs: sent to the server as stored, and
  compared exactly (a key compared without its fraction of a second used to drop untouched rows). A table with a row-level security filter, or
  masked columns the login may not see, is refused: the stream has every row.
* **Requests:** `logminer.dbo.snapshot_requests` (one table, every table, or `'*'` databases), with progress and status
  written back. `CaptureEngine.RequestSnapshot` does the same from code.
* **Identical encoding:** snapshot values are converted to the CLR types the log decoder produces. `money` keeps scale 4 on
  both paths, and text is read as bytes and decoded like the log's. A real-engine test asserts that every snapshot row is byte-identical, as
  JSON, to the row's stream image. Off-row LOBs, `xml` and legacy LOBs are values in a load and unavailable in the stream.

### Comparing tables with the stream
`logminer-verify` (`src/LogMiner.Verify`) reads the stream as a consumer would, and nothing else: no SQL Server, nothing written, no acknowledgements.
* **Chain:** the before image of every update and delete must be the row as the stream left it; an inserted key must not be there.
* **Load:** every row of a historical load must be the row the stream has built, what the load does not mention must not be there, and its count must
  arrive. So requesting a load compares the whole table with the stream, at its place in the stream, without stopping the writers.
* Rows are kept as hashes (100-260 bytes each in that test), exit code 1 for any difference.
* *Measured* end to end (`StreamVerificationTests`: SQL Server, engine, NATS; writers, key changes, rollbacks, a column added, a restart, three loads):
  5,507 messages, all three tables verified; **600 of 600** events taken away one at a time are noticed.
* About 120,000 messages/s from a local NATS server and 82 bytes per row of five columns (*measured*, a million rows).
* It cannot tell: what happened before a table's first whole load, tables without a key, a lost update that a later one hides in columns that are
  unavailable or were retyped in between, a delete that is missing at the very end of the stream (reported as undecided until the next message). It may
  report too much: when the first message the stream still holds is in the middle of a replay, for DDL that changes a value's text but not its JSON
  kind, for a column dropped and added again. A renamed table is another table.

## 5. Self-test: is this SQL Server build understood?
The log's format is not a documented interface. Before capturing, the service proves once per (SQL Server version, miner build) that it
still works here: in a scratch database it checks every type, rollbacks and savepoints, heaps with forwarded rows, DDL while the capture is
down, the release bookmark, and that historical loads encode rows exactly like the stream, between their markers (2 s). The result is kept in
`logminer.dbo.self_tests`. A failure stops the service before it captures anything (`SelfTest`: `Require` | `Warn` | `Off`); the log is
retained meanwhile. Before patching: `logminer.exe --self-test "<connection string of a test server with the CU>"` (exit code 0 or 4).

## 6. Tests

Property-based with [Hegel](https://hegel.dev) (C# 0.4.1). Check the number of tests that ran, not only that none failed: a test run that is
killed (a `timeout` around it) says "Test host process crashed" and reports the tests that finished before. Hegel takes synchronous bodies
only; `tests/Shared/Blocking.cs` is the one place that blocks on asynchronous code.

| Test | Proves |
|---|---|
| `LsnProperties` | text/binary round trips, ordering = byte order |
| `RowDecoderProperties` | random layouts × values decode exactly; arbitrary bytes only raise decoder exceptions |
| `TransactionAssemblerProperties` (stateful) | interleaved transactions, savepoints, aborts → exactly the committed effects, commit order, restart LSN |
| `SpoolProperties` (stateful) | sends, replays, remote failures (stored / not stored / half / throw), **asynchronous outcomes reported late**, restarts, **crashes before fsync with torn tails + garbage** → no phantoms, per-stream order, every acknowledged message delivered |
| `CapturePipelineProperties` (stateful) | the capture + checkpoint coordinator (± spool) against a simulated log with replication-accurate truncation and error-9005 behaviour, faulty sink, crashes, saves interleaved with pumps, **`Latest` starts over existing history, historical loads with commits, updates and deletes landing inside chunk windows, failing chunk reads and lost chunk sends, DDL (rebuilds, new physical columns; inside transactions, rolled back, racing the catalog read), undo of changes that were never captured, log released by someone else** → gaps are never silent and never invented; every committed change delivered once (after dedupe), correct images, commit order, no gap once a stream has started; a replica built from stream + snapshot events equals the committed table; `sp_repldone` never beyond the durable checkpoint |
| `EngineRoundTripProperties` (integration) | **the real engine as oracle**: random DML over every supported type on fresh TDE + CDC databases; a replica rebuilt from the published JSON equals `SELECT *` after every step, across log backups, `sp_repldone`, key changes and restarts; **historical loads emit every row once, byte-identical to its stream image** |
| `HistoricalLoadTests` (integration) | `Latest` start over 300 existing rows + 3 concurrent writers (insert / update / delete) during a load with chunks of 1, 3 and 17 rows → the replica built from all events equals the table |
| `SchemaEvolutionProperties` (integration) | **the real engine as oracle for DDL**: random add / drop / alter column, rebuilds, re-clustering, rolled-back DDL and row changes, while the miner lags, restarts, loses its sink and truncates the log; heaps, non-unique clustered keys, row versioning, accelerated database recovery → every change is delivered once, in order, with exactly the columns and values the row had at that moment |
| `SchemaEvolutionTests` (integration) | every DDL kind both while the miner is down and live; one transaction mixing DDL and row changes; savepoint rollback of DDL; dropped tables; CDC disabled meanwhile; writers during online rebuilds; catalog page splits; a DDL storm racing the pump; LOB work undone to a savepoint |
| `GapTests` (integration) | the bookmark keeps its log; foreign `sp_repldone` and CDC disabled → gap recorded once, capture goes on; full release → later truncation reported; foreign release reported live; CDC off and on for one table and tables captured later are reported, replacing a capture instance without a break is not |
| `LogFlushDecoderProperties` | generated packets decode in order; damaged or arbitrary ones are refused as a whole; fields are found by the column list; recorded packets of the real engine account for every byte written |
| `LogWriteAuditProperties` | writes, late, early and lost events in any order: a lost one is found by the next poll, a pushed one is not read again; outages; stalls |
| `DoorbellTests` (integration) | commits pushed within milliseconds with their log block; session dropped, stopped, robbed of its event, stream or dispatcher killed → still noticed, and the push comes back; missing permission; stuck reader; prompt stop; **stateful: with the sweep off the doorbell alone must wake the capture while all of that happens** |
| `CapturePipelineProperties` (more) | a checkpoint taken while a transaction is being prepared, then a fault or a crash; a transaction that cannot be decoded holds back what follows and nothing else; a restart position moved anywhere later loses nothing; read and batch sizes are drawn; **power failures of the destination (what it had acknowledged and not synced is gone), with the release interval; log blocks, and the log beginning inside the block of the position; a consumer that uses load markers: behind gaps, too, its replica equals the table** |
| `EventProperties` | message ids are equal for the same change only (any names, servers, incarnations, readings); any nvarchar value as JSON, any varchar bytes in 17 code pages, numbers bit by bit; headers, destinations, oversized events; keys equal exactly when their values are |
| `DurabilityProperties` | spool records and their damage; spool directories with files flipped, cut, zeroed, deleted, renamed: everything acknowledged is delivered or it refuses to start; checkpoint files; event files |
| `StreamIntegrityTests` (integration) | a commit that is not on disk is not published; a start that is not in the log is refused; every kind of partial rollback under RCSI and ADR, with SQL Server's own record counts; undone transactions are released; ordinary truncation is not a gap |
| `DatabaseIdentityTests` (integration) | names swapped, ids reused, restored over itself, not openable; **stateful: random renames, restores, drops, detach/attach, offline while rows are written → a row is only published under a name its database had, and none is missing** |
| `HistoricalLoadKeyTests` (integration) | 16 kinds of key (fractions of a second, NULLs, Cyrillic, UTF-8, Japanese, Turkish, half surrogate pairs, 38 digits, CLR …): every row once for every chunk size, the stream's JSON, no row dropped because of another |
| `UnloggedChangeTests` (integration) | SWITCH in and out, live and while down, is a gap once; rebuild, split, merge are not; converting DDL is reported; large rebuilds and drops; a DDL transaction whose beginning is truncated |
| `JetStreamSinkTests` (integration) | a real server stopped, restarted, purged, put back, its stream deleted, others answering: stored in order, nothing late, only its acknowledgement counts, the audit notices; **a power failure that takes acknowledged messages: published again by itself** |
| `ServiceTests` (integration) | the real `logminer.exe`: no NATS at boot, lease killed, NATS stopped, state database offline, a captured database offline, every session killed → the same process delivers everything; killed and started again; a standby takes over |
| `SelfTestTests` (integration) | passes here, leaves nothing behind, never drops a database it did not create |
| `StreamVerificationTests` (integration) | tables against the stream, end to end, with `logminer-verify`: nothing differs, and every event that is taken away is noticed |
| `StreamVerifierProperties` | the verifier against simulated tables, streams and losses |
| `SqlStateStoreTests` (integration) | checkpoints round trip; standby takes over when the active instance dies; a fenced instance cannot overwrite |

**Mutation-tested**: planted bugs that the tests must catch.
* **Stream and spool (8 of 8 caught):** checkpoint ignoring open transactions, release beyond the durable checkpoint,
  ignoring compensation records, acknowledging before the sink acks, skip-filter off-by-one, ack before fsync, no go-back
  after a failed batch, pipelined forwarder.
* **Historical loads (5 of 5 caught):** keeping rows changed inside [L, H), advancing the resume key before the ack,
  emitting a chunk before the stream reaches H, a finished load keeping its slot, and a snapshot `date` converted
  differently from the decoder (caught by the real-engine byte-identity check).
* **`Latest` start (2 of 3 caught):** checkpoints during the catch-up and publishing before the start point is durable
  were caught. The third mutant, a reset that ignores the in-memory start point, is equivalent: nothing can fault while
  publishing is held, so that fallback is defensive only.
* **Layouts as of an LSN and undo (14 of 15 caught):** decoding with the current layout, skipping unknown rowsets, loading layouts without the
  open transactions, reloading after the DDL transaction or not at all, compensation cancelling the newest or one change too many, savepoint
  rollbacks or in-place changes of the catalog ignored, page splits taken for DDL, defaults of dropped rowsets, `date` defaults, tables no
  longer captured, collations as of now. The survivor (aborted DDL not excluded) is equivalent: the abort's undo records cancel it anyway.
* **Gaps and bookmark (9 of 9 caught):** truncation passed over silently, releasing the newest transaction, a full release not marked in the
  checkpoint or done while behind, cut transactions published or skipped without a record, gap not recorded, a table's CDC off/on not
  noticed, an additional capture instance taken for a break.
* **Self-test (3 of 3 caught):** compensation ignored, current layout used, `money` decoded differently.
* **Doorbell (32 of 33 caught):** audit, decoder, forced dispatch, fallback, restart, session handling, retention mode, repeated reads. The
  survivor (an explicit "nothing is attached any more" check) duplicates the error of the stream itself.
* **Position (5 of 5 caught):** moved back by re-reading or by a fault during a save; the cut marker not saved, not restored, not stored.
* Five of these survived the first run and were only caught after the tests were strengthened (a model that tolerated self-healing faults, no
  `DROP COLUMN` inside a transaction, no real mid-page split, no second capture instance created while down).

Bugs the tests found in the implementation: `decimal` scale > 28; overtaking after a synchronous send failure; overtaking
with pipelined batches; position-based dedupe dropping a never-spooled message; `money` stored as plain little-endian int64
(not the TDS layout); trailing zero-length variable columns are empty, not NULL; schema reload on the reader's busy
connection; pooled lease connection keeping the lock; doorbell `smallint` cast silently disabling the signal; a finished
historical load whose last chunk went out in a mid-pump batch kept its slot (the next load never started and the database
was re-pumped forever); a `Latest` start could leave a gap or deliver older history after newer events (checkpoint during the
catch-up, or a fault before the first checkpoint); **everything written before an index rebuild, a re-clustering or a `DROP TABLE` was
dropped silently when the log was read after it** (restart, sink outage, lagging), and `ALTER COLUMN` published NULLs; rolling DDL or LOB work
back to a savepoint cancelled unrelated earlier changes of the transaction; a log released by someone else was skipped silently; CDC switched
off and on for a table lost the changes in between silently; metadata-only `date` defaults came out as datetimes; a checkpoint written while
the log was re-read after a restart, or a sink failure during a checkpoint save, moved the durable position back (duplicates, and after a
truncation a gap that was none); a restart right after a truncated position forgot that transactions were cut and published the rest of one.

## 7. Measured end-to-end (lab host, 1,200 databases, 200 transactions/s spread over all of them)

`lab service`: `logminer.exe`, the load and the consumer are three processes, 40 s of load after start-up. Latency is from the application
issuing the insert to the consumer receiving the event from JetStream.

| | push, 4 ms (default) | push, 2 ms | poll only (100 ms) |
|---|---|---|---|
| events | 8,001 / 8,001, 0 duplicates | 8,001 / 8,001, 0 duplicates | 8,001 / 8,001, 0 duplicates |
| p50 | **3.7 ms** | 2.6 ms | 6.2 ms |
| p90 | **5.4 ms** | 3.8 ms | 69.9 ms |
| p99 (three runs) | **6.7, 6.7, 7.3 ms** | 4.8 ms | 105.6 ms |
| p99.9 | 13–19 ms | 16 ms | 111 ms |
| max | 26–30 ms | 28 ms | 112 ms |
| events over 20 ms | 2–5 | 3 | 1,828 |
| CPU of its SQL sessions (one core) | 3.3–4.4 % | 5.6 % | 6.7 % |
| CPU of `logminer.exe` (one core) | 14–15 % | 16 % | 12 % |
| memory of `logminer.exe` | 156 MB | 166 MB | 163 MB |

* 8 ms dispatch (`lab e2e`): p50 5.7, p99 10.3 ms. 1,000 tx/s at 4 ms: p50 3.7, p99 6.3, p99.9 16.4 ms.
* Of the latency, 0.7 ms (p99 1.3 ms) is encode → JetStream → consumer.
* Earlier tables (p99 28–106 ms) came from a benchmark whose load generator opened a connection per insert in the miner's own process; the
  poll column above is the fair comparison.
* First start (`Latest`), 1,200 databases: 23–29 s.
* **Under that load, event session dropped at 10 s, `logminer.exe` killed at 20 s and restarted:** 8,001 of 8,001 events, 0 duplicates. After
  the restart every database reloads its layouts: the first ones are current within a second or two, the last after ~18 s.

## 8. Operations

* `max text repl size = -1`; CDC enabled per captured table; capture/cleanup jobs dropped (`infra/sqlserver`).
* State database `logminer` (`infra/sqlserver/setup-logminer-db.sql`); back it up with the instance.
* The push needs `ALTER ANY EVENT SESSION` (and `VIEW SERVER STATE`, which polling needs too; `experiments/23`). Without it the service polls.
* Event session `logminer_doorbell_<ServerName>`: created on start, dropped on stop, replaced after a crash. Nothing is written to disk.
* Instance sizing at 1,200 databases (*measured*: 8 GB ran out at ~1,030 databases with Query Store on; not the miner).
* Monitor: faulted streams, untracked tables, sink health, rows in `logminer.dbo.gaps`, foreign releases, and the *size* of log held
  (`sys.dm_db_log_stats.active_log_size_mb` where `log_reuse_wait_desc = REPLICATION`). The wait reason alone is normal: the bookmark holds
  the newest captured transaction. The miner being down retains log by design: alert on it, and do **not** run `sp_repldone` to free it.
* A stream that stops with `UndecodableChangeException` (a layout that cannot be derived) keeps its log. To give up the undecodable part:
  stop the service, `EXEC sp_repldone NULL, NULL, 0, 0, 1; EXEC sp_replflush` in that database, delete its row in `logminer.dbo.checkpoints`,
  start the service, request a historical load.
* JetStream: R1 loses on a NATS host power loss what was acknowledged since the last sync. `sync_interval: always` is not the answer: *measured*
  p50 6.5 s at 200 events/s (every message is synced on its own), `100ms` +0.8 ms, `1s` nothing measurable. Instead a change stays in SQL
  Server's log for `ReleaseIntervalMs` (10 s) after its checkpoint, however many follow, so with a shorter `sync_interval` what NATS lost is
  still there, and is published again by itself (section 3). Consumers see it as a repetition. R3 (three hosts) has no such window.
* Check the stream against the tables: request a load (README) and run `logminer-verify`.
