# The NATS stream: subjects, message format, ordering, delivery guarantees

What a consumer of logminer's JetStream stream gets, and what it can rely on. Each captured row change is one JSON message. This
page describes the format and the guarantees, and gives a recipe for a consumer that applies the changes correctly. Why the guarantees hold is in
[design.md](design.md#3-durability--chain-of-custody), section 3.

All examples on this page are real messages, from a scratch database `lm_doc_sample` captured as server `sql01`; `...` marks where they are
shortened.

## In short

| | |
|---|---|
| **Completeness** | Every committed row change of a captured table that is in the log is published. Uncommitted and rolled-back work never is. Some events can make changes go missing: someone else released the log, a restore, CDC switched off and on for a table, a renamed table, `ALTER TABLE ... SWITCH`. Each is recorded in `logminer.dbo.gaps` before the stream goes on. By default the affected tables are then loaded again into the stream ([historical loads](#historical-loads-in-the-stream)). DDL that gives existing rows a value without writing them only produces a warning in logminer's log ([schema changes](#schema-changes)). |
| **Start** | The stream begins where capture began (`StartPosition`). Rows that existed before that reach a consumer only through a historical load, so a new consumer starts with one. |
| **Delivery** | At least once. JetStream drops a message sent again with the same `Nats-Msg-Id` within the stream's duplicate window (10 minutes). After the window, a duplicate is stored again. |
| **Order** | Per database (default): the stream holds a database's events in commit order, and within a transaction in `seq` order. There is no order between databases. |
| **Transactions** | The events of a transaction are consecutive in their database's order and share `commit_lsn`; `txn_size` says how many there are. NATS does not deliver them atomically: a reader can see the first before the last has arrived. |
| **Durability** | SQL Server is allowed to truncate log only behind events that JetStream acknowledged, whose position is durable, and that the stream was checked to still hold. Until then, everything can be published again from the log. |
| **Retention** | The stream logminer creates keeps messages for 7 days (`MaxAgeDays`). A consumer that is further behind has lost messages. It must catch up from a [historical load](#historical-loads-in-the-stream). |

## Stream and subjects

logminer creates the stream when it is missing (`ManageStream`, default `true`), and never changes one that exists. The stream it creates:

| | |
|---|---|
| name | `LOGMINER` (`LogMiner:Sink:NatsJetStream:Stream`) |
| subjects | `cdc.>` (`Subjects`) |
| storage, retention | file; limits, oldest discarded first |
| age | 7 days (`MaxAgeDays`) |
| duplicate window | 10 minutes (`DuplicateWindowMinutes`) |
| replicas | 1 (`Replicas`; 3 on a three-server cluster) |
| largest message | 8 MiB, but never more than the server's `max_payload` (1 MiB by default) |

A message's subject is rendered from `LogMiner:Routing:DestinationTemplate`, by default `cdc.{server}.{db}.{schema}.{table}`; `{op}` is
available too. For example: `cdc.sql01.lm_doc_sample.dbo.orders`. `{server}` is `LogMiner:ServerName`, the same for every instance that captures
one SQL Server.

- **Filtering:** one table of every database is `cdc.sql01.*.dbo.orders`; one database is `cdc.sql01.tenant_0042.>`.
- **A subject is not an identity.** Each token keeps `A-Z a-z 0-9 _ -`, and every other character becomes `_` (`.` and spaces, too, so the
  subject stays valid). Tables `order.items` and `order_items` therefore share a subject. Identify a message by its payload (`source.db`,
  `source.schema`, `source.table`) or headers, not by the subject.

## A message

### Headers

```
Nats-Msg-Id: sql01:lm_doc_sample:608E0A435D99:0000003D:00000558:0046:0
Nats-Expected-Stream: LOGMINER
Content-Type: application/json
lm-db: lm_doc_sample
lm-table: dbo.orders
lm-op: c
lm-commit-lsn: 0000003D:00000558:0046
```

| Header | |
|---|---|
| `Nats-Msg-Id` | Deterministic id: identical when the same change is published again, and never that of another change of another server, database or incarnation. JetStream uses it to drop duplicates in its window. A change: `server:db:incarnation:commit_lsn:seq`. A row of a load: `server:db:incarnation:snap:<reading>:<n>`, plus `:begin` and `:end` for its markers. Names are percent-escaped, ASCII. |
| `Nats-Expected-Stream` | The stream the message was published for. A server that would store it in another stream refuses it instead. |
| `lm-db`, `lm-table`, `lm-op` | Database, `schema.table` and operation, so a consumer can route or filter without parsing the payload. Control characters, `%`, and spaces at the ends are percent-encoded. |
| `lm-commit-lsn` | The transaction's commit position (changes only, not rows of a load). |

### Payload

An insert, the first of two in one transaction:

```json
{
  "op": "c",
  "key": { "id": 1 },
  "before": null,
  "after": { "id": 1, "customer": "Ada", "amount": "120.50", "placed": "2026-11-26T10:15:00.2500000", "paid": false, "ref": "c4b1a6f0-4f3e-4d2a-9b7e-2f0c5a1d9e11" },
  "source": {
    "server": "sql01", "db": "lm_doc_sample", "incarnation": "608E0A435D99", "schema": "dbo", "table": "orders",
    "lsn": "0000003D:00000558:0044", "commit_lsn": "0000003D:00000558:0046", "begin_lsn": "0000003D:00000558:0029",
    "xid": "0000:00000457", "seq": 0, "txn_size": 2, "commit_time": "2026-11-26T09:38:23.487Z"
  },
  "ts_ms": 1795685903506
}
```

| Field | |
|---|---|
| `op` | `c` insert, `u` update, `d` delete, `r` a row of a historical load, `l` a load's marker. Consumers ignore operations they do not know. |
| `key` | The primary key (or unique clustered index) columns of the row, from `after` (from `before` for a delete). `null` for a table without one, and for load markers. |
| `before` | The whole row before the change: `u` and `d`. Otherwise `null`. |
| `after` | The whole row after the change: `c`, `u` and `r`. Otherwise `null`. |
| `load` | Only on `l`: `{"id", "phase": "begin" \| "end", "rows"}` (`rows` at the end). |
| `source.server` | `LogMiner:ServerName`. |
| `source.db`, `.schema`, `.table` | Where the row is: the names the table had **when the change was made**. |
| `source.incarnation` | The database's incarnation (its recovery fork and creation time). It changes when the database is restored or re-created. Positions can only be compared within one incarnation. |
| `source.lsn` | The log position of this row's change. |
| `source.commit_lsn` | The transaction's commit position: its place in the database's commit order. Absent on rows and markers of a load. |
| `source.begin_lsn` | Where the transaction began. |
| `source.xid` | SQL Server's transaction id (as `fn_dblog` shows it). |
| `source.seq` | The change's place in its transaction: ascending from 0. Numbers can be skipped when the transaction also changed replicated rows that are not captured. Absent on a load's rows and markers. |
| `source.txn_size` | How many events the transaction has. On a row of a load: the rows of its chunk. |
| `source.commit_time` | When the transaction committed, in UTC (SQL Server logs local time; logminer converts it with the server's time zone rules), to the millisecond. |
| `source.snapshot` | Only on `r` and `l`: the load's id. |
| `ts_ms` | When logminer encoded the message (its own clock, Unix milliseconds). It says nothing about order. |

**Positions** (`lsn`, `commit_lsn`, `begin_lsn`) are three fixed-width upper-case hex numbers, `VLF:block:slot`. Compared as strings, they are in
log order, so `(commit_lsn, seq)` orders a database's changes (within one incarnation).

### Operations

An update carries both images:

```json
{ "op": "u", "key": { "id": 1 },
  "before": { "id": 1, "customer": "Ada", "amount": "120.50", "placed": "2026-11-26T10:15:00.2500000", "paid": false, "ref": "c4b1a6f0-..." },
  "after":  { "id": 1, "customer": "Ada", "amount": "118.00", "placed": "2026-11-26T10:15:00.2500000", "paid": true,  "ref": "c4b1a6f0-..." },
  "source": { ..., "commit_lsn": "0000003D:00000570:0003", "seq": 0, "txn_size": 1 } }
```

**An update that changes the key arrives as a delete and an insert** of the same transaction (`UPDATE dbo.orders SET id = 3 WHERE id = 2`):

```json
{ "op": "d", "key": { "id": 2 }, "before": { "id": 2, "customer": "Grace", ... }, "after": null, "source": { ..., "commit_lsn": "0000003D:00000578:0005", "seq": 0, "txn_size": 2 } }
{ "op": "c", "key": { "id": 3 }, "before": null, "after": { "id": 3, "customer": "Grace", ... }, "source": { ..., "commit_lsn": "0000003D:00000578:0005", "seq": 1, "txn_size": 2 } }
```

That is how SQL Server logs it. Consumers should not expect a `u` with two different keys.

A transaction that is rolled back (or the part of one undone by a savepoint rollback) publishes nothing.

### Values

| SQL Server | JSON | Example |
|---|---|---|
| `bit` | boolean | `true` |
| `tinyint`, `smallint`, `int`, `bigint` | number | `42` (beyond 2^53, a `bigint` loses precision in JavaScript's `JSON.parse`) |
| `real`, `float` | number | `0.25` |
| `decimal`, `numeric`, `money`, `smallmoney` | string, exact, with the column's scale | `"120.50"` |
| `date` | string | `"2026-11-26"` |
| `time` | string | `"10:15:00.0000000"` |
| `datetime`, `datetime2`, `smalldatetime` | string, without a zone (as stored) | `"2026-11-26T10:15:00.2500000"` |
| `datetimeoffset` | string with offset | `"2026-11-26T10:15:00.0000000+01:00"` |
| `char`, `varchar`, `nchar`, `nvarchar` | string | `"Ada"` |
| `uniqueidentifier` | string | `"c4b1a6f0-4f3e-4d2a-9b7e-2f0c5a1d9e11"` |
| `binary`, `varbinary`, `rowversion`, `hierarchyid`, `geometry`, `geography` | base64 of the stored bytes | `"AAAAAAAAB9E="` |
| `xml`, `sql_variant`, `text`, `ntext`, `image`, an off-row (`max`) value | not in the log: `{"unavailable": true}` | |
| `NULL` | `null` | |

- **`{"unavailable": true}` means "not known", not "empty".** A value that is not in the log (see the table), or that would make a message larger
  than the server accepts, comes like this, never in a key column. In a load the same columns have their values. A consumer keeps what it had
  for that column instead of overwriting it.
- `char`/`varchar` bytes that are not valid text in the column's code page come as `{"undecodable": "<base64>"}`. An `nvarchar` holding half a
  surrogate pair comes as a `\ud83d` escape, exactly as stored.

### Schema changes

There are no DDL messages. Each image has the columns its table had **at the moment of the change**. Changes made before a DDL statement keep
the old shape, even when they are published after it. Treat the set of columns as open. Measured on SQL Server 2022:

| DDL | What arrives |
|---|---|
| `ADD c ... NULL` | later images have `c` (`null` until a row is given a value) |
| `DROP COLUMN c` | later images do not have `c` |
| `ADD c ... NOT NULL DEFAULT x` | later images have `c`. The rows that existed get `x` **without being written**: nothing is published for them, no gap is recorded, no load is requested. A row's next change shows its value. logminer logs a warning: `column c was added with a value for the rows that existed` |
| `ALTER COLUMN` that converts values (`int` to `bigint`, ...) | later images have the new type. The existing rows are converted **without row changes in the log**: as above, with the warning `the values of column c were converted from ... to ...` |
| `sp_rename` of a column | refused by SQL Server on a CDC table (Msg 4928, `Cannot alter column ... because it is 'REPLICATED'`) |
| `sp_rename` of the table | later changes come under the new name, on the new subject. A gap `Renamed` is recorded, and by default a load of the table under its new name follows, because the consumer's rows are under the old one |

After either warning, a consumer that needs the existing rows' new values applies the DDL to its copy, or requests a load of the table. A table
that is dropped publishes nothing more.

## Ordering

- **Per database, in commit order** (`LogMiner:Routing:PartitionKey` = `Database`, the default). logminer publishes one database's messages one
  at a time: the next goes out only after JetStream acknowledged the one before. So a lost message is never overtaken by a later one, and the
  stream sequence of one database's messages follows `(commit_lsn, seq)`. Databases are independent: their messages interleave in no particular
  order.
- **Within a transaction**, in `seq` order (log order), and consecutive: no other transaction of the database comes in between.
- **A subject filter keeps the order.** A consumer of one table's subject sees that table's changes in commit order, but only part of each
  transaction, so `txn_size` counts events it will not see.
- **Rows of a load** come at their place in the database's order, between its transactions (see below).
- **`PartitionKey` `Table` or `RowKey`** orders per table or per row instead. One transaction's messages can then be stored in another order,
  and a consumer has to keep its position per table or row. The service logs a warning, and loads cannot be checked by `logminer-verify`.
- **Order to the consumer is the consumer's affair.** JetStream delivers a consumer's messages in stream order. A consumer with several messages
  in flight (`MaxAckPending` > 1 with parallel workers), or several consumers on one database, can apply them out of order. Apply one
  database's messages in order (for example, one ordered worker per database).

## Delivery guarantees

### The chain

1. The change is in SQL Server's log, and SQL Server keeps that log (CDC holds it for replication) until logminer releases it with
   `sp_repldone`.
2. logminer reads it only once it is on disk, publishes it, and waits for JetStream's acknowledgement (`PubAck`) from the configured stream. A
   subscriber that happens to answer does not count, and neither does another stream.
3. It saves its position, durably and fenced by its lease, in the state database on the instance.
4. It releases the log only behind such a position. Before that, it checks that the stream still holds the newest message it acknowledged, under
   its id. If the stream lost acknowledged messages (a NATS host lost power before its `sync_interval`), logminer goes back to a safe position
   and publishes them again by itself. Until it can, it releases nothing, and the log says `THE LOG OF ... DATABASE(S) IS NOT RELEASED`.

So if logminer, NATS or the network fails at any point, the change is still in SQL Server's log and is published again.

### When a message comes more than once

- logminer restarted, or a failure rewound a database to its last durable position (at most about a checkpoint interval of messages). Within the
  duplicate window, JetStream drops these by `Nats-Msg-Id`.
- The resend comes later than the window: after an outage longer than 10 minutes, or when logminer republishes what a NATS server lost. Then
  the duplicate is stored again, *behind newer messages*.
- A chunk of a historical load is read again. Its rows get a new id (each reading is different), so they are stored again. They are upserts.
- The consumer's own redelivery: JetStream redelivers what a consumer did not acknowledge in time.

Never: a message that was not acknowledged and is skipped, or a later message of a database stored in front of an earlier one that was lost.

### When changes may be missing

Some changes cannot be published at all:
- log released by someone else (a CDC capture job, `sp_repldone` by hand)
- a database restored or re-created (a new `incarnation`)
- CDC disabled and enabled again on a table
- rows moved by `ALTER TABLE ... SWITCH`
- a renamed table

logminer records each in `logminer.dbo.gaps` before the stream goes on. With `OnGap: Resync` (the default) it requests a historical load of the
affected tables in the same transaction. The stream then carries a complete load, from which a consumer can correct its copy, deletes
included. The stream itself has no gap message: the load is the signal, and `logminer.dbo.gaps` says why.

## Historical loads in the stream

A load is requested in the state database (see the [README](../README.md#historical-loads)), or by a gap. It runs inside the database's live
stream, without locking the table, and publishes on the table's subject:

```json
{ "op": "l", "key": null, "before": null, "after": null, "load": { "id": "req1.dbo.orders", "phase": "begin" },
  "source": { "server": "sql01", "db": "lm_doc_sample", "incarnation": "608E0A435D99", "schema": "dbo", "table": "orders", "lsn": "0000003D:00000580:0007", "snapshot": "req1.dbo.orders" } }
{ "op": "r", "key": { "id": 1 }, "before": null, "after": { "id": 1, "customer": "Ada", "amount": "118.00", ... },
  "source": { ..., "lsn": "0000003D:00000588:0001", "txn_size": 1, "snapshot": "req1.dbo.orders" } }
{ "op": "l", "key": null, "before": null, "after": null, "load": { "id": "req1.dbo.orders", "phase": "end", "rows": 1 },
  "source": { ..., "snapshot": "req1.dbo.orders" } }
```

- **Between `begin` and `end`, every row the table has at the end was either loaded (`r`) or changed (`c`/`u`).** A key the consumer holds that
  was neither does not exist any more: delete it. This is how deletes inside a gap are found.
- `rows` is the number of rows loaded. If fewer different keys arrived, the load is not whole: apply its rows, but delete nothing on its account.
- A load that starts over (a restart, a fault) begins again with the same `id`. Rows can arrive more than once; they are upserts.
- **`r` rows have no `commit_lsn` and no `seq`.** They have no place in the commit order. They neither move a consumer's position nor are
  compared with it. A consumer that did so dropped a change that came right behind a load.
- A table needs a primary key or a unique clustered index to be loaded.

## Writing a consumer

Per database incarnation `(source.server, source.db, source.incarnation)`, keep a **position**: the last `(commit_lsn, seq)` applied. Store it
together with what you applied (one transaction), so the two cannot disagree after a crash. Then:

```
for each message, in stream order (one database's messages in order):
    stream = (source.server, source.db, source.incarnation)
    switch op:
      c, u, d:
          pos = (source.commit_lsn, source.seq)          # strings compare in log order
          if pos <= position[stream]: acknowledge; continue    # a duplicate: applied before, changes nothing below
          c: upsert after          u: upsert after (keep columns that are {"unavailable": true})
          d: delete key
          position[stream] = pos
          if the table has an open load: add key to its touched keys (c, u)
      r:  upsert after                                  # position untouched
          if the table has an open load with this id: add key to its loaded keys (a set: a re-read chunk counts once)
          # an r after its load's end (a chunk read again) is only an upsert
      l:  begin: if a load with this id is open: start it over (both sets empty again)
                 else open it: loaded keys = {}, touched keys = {}
          end:   if no load with this id is open: ignore    # it began before you started reading
                 if size(loaded keys) >= load.rows:        # the load is whole
                     delete every key of the table that is in neither set
                 # else: rows of the load are missing; delete nothing
                 close the load
      other: ignore
    acknowledge
```

This is what `logminer-verify` does (`src/LogMiner.Verify/StreamVerifier.cs`).

- **One position per incarnation.** A restored database starts a new incarnation; positions of the old one mean nothing for it, and a load
  follows by default.
- **Transactions as a whole:** to apply them atomically, buffer a database's events until `txn_size` of them with the same `commit_lsn` have
  arrived (consecutive in the stream). This works only when the consumer sees every table of the database.
- **With `PartitionKey` `Table` or `RowKey`:** keep the position per table, or per row, instead.
- **Too far behind:** if the consumer's position is older than what the stream still holds (`MaxAgeDays`), request a load of the tables
  instead of reading on.
- **Check it:** `logminer-verify` reads the stream as a consumer and reports every place where it does not match its tables (see the
  [README](../README.md#does-the-stream-match-the-tables)).

## What is not guaranteed

- **Exactly once.** Deduplicate by position, as above.
- **Order between databases**, and order between tables with `PartitionKey` `Table` or `RowKey`.
- **Atomic transactions** in NATS: a reader can see part of one.
- **Values that are not in the log** (`xml`, `sql_variant`, legacy and off-row LOBs) in changes: `{"unavailable": true}`. Loads have them.
- **A gap message in the stream.** The load that follows is the signal; `logminer.dbo.gaps` says what and why.
- **Rows that DDL changes without writing them** (`ADD ... NOT NULL DEFAULT`, an `ALTER COLUMN` that converts values): only a warning in
  logminer's log. Apply the DDL to your copy, or request a load.
- **Rows from before the capture began**: only a load brings them.
- **A stable subject per table.** A rename moves it, and different names can map to one subject.
