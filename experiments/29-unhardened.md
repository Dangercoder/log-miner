# Can the log reader see a commit that is not on disk yet?

Yes. `ALTER DATABASE x SET SUSPEND_FOR_SNAPSHOT_BACKUP = ON` (held by its own session) freezes the database's log writes. Another session then
commits a transaction (2022 CU28, 2026-11-24):

| while the log write is frozen (session waits for WRITELOG, its client has no answer) | after resuming |
|---|---|
| `log_end_lsn` 00000039:000001B0:0001 | 00000039:000001B8:0001 |
| `fn_full_dblog` returns the BEGIN, INSERT and **COMMIT** records at 00000039:000001B0:0001..0004 | the same |

So `fn_full_dblog` reads log blocks from memory before they are hardened, and `sys.dm_db_log_stats.log_end_lsn` is the end of the hardened log
(the start of the first block that is not on disk). A miner that reads beyond it can publish a transaction that a crash un-happens, and the
LSNs it read are then used again: the real transactions there would be taken for already read, and their events (same message ids) for
duplicates. Reads therefore stop at the `log_end_lsn` seen before the scan.

Statements: `setup.sql` creates the database; session 1 `ALTER DATABASE lm_hard SET SUSPEND_FOR_SNAPSHOT_BACKUP = ON; WAITFOR DELAY '00:00:08';
ALTER DATABASE lm_hard SET SUSPEND_FOR_SNAPSHOT_BACKUP = OFF;` session 2 `BEGIN TRAN hardening; INSERT dbo.t VALUES (2, 'hardening'); COMMIT;`
session 3 (from master) reads `log_end_lsn` and `fn_full_dblog`. The suspension ends with its session, so session 1 has to stay connected.
