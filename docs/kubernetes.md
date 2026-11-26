# Running logminer in Kubernetes

This is the path from a fork of this repository to logminer capturing a SQL Server instance from a Kubernetes cluster and publishing to NATS
JetStream. Everything a pod needs comes from environment variables: no file to edit inside the image, and no volume.

```
 SQL Server instance ──TCP+TLS──▶ logminer pod (active)  ──▶ NATS JetStream (stream LOGMINER, subjects cdc.<server>.<db>.<schema>.<table>)
 (1 database per tenant,          logminer pod (standby: waits for the lease, takes over in ~1 s)
  state database `logminer`)
```

One Deployment per SQL Server instance, in a namespace of its own, with two pods. One holds the lease (an application lock in the instance's state
database) and captures. The other waits and takes over when the first one goes away. Positions, gaps and requests for historical loads are kept
in SQL Server, so a pod can be replaced at any time and nothing is lost. What changes when logminer runs off the database host (latency, the
login's rights, failover times) is measured in [design.md](design.md#off-the-database-host-a-container-kubernetes).

| Step | Once per |
|---|---|
| [1. Fork and build the image](#1-fork-and-build-the-image) | fork |
| [2. Prepare SQL Server](#2-prepare-sql-server) | SQL Server instance |
| [3. Prepare NATS](#3-prepare-nats) | NATS account |
| [4. Configure](#4-configure) and [5. deploy](#5-deploy-and-check) | SQL Server instance |

[Settings](#settings) lists every setting. [When something is wrong](#when-something-is-wrong) covers the common failures.

## 1. Fork and build the image

1. Fork the repository on GitHub. **A fork of a public repository is public.** `.gitignore` keeps secrets out of git (`secret.env`, `*.creds`,
   `*.pem`, `*.key`, `*.cer`, ...); never force them in. If even your host names must not be public, don't fork: import the repository into a
   private one (GitHub: *New repository → Import*).
2. Enable GitHub Actions in your fork (the *Actions* tab). [.github/workflows/container.yml](../.github/workflows/container.yml) builds the image
   and publishes it to your fork's GitHub Container Registry:

   | Event | Image |
   |---|---|
   | push to `master` | `ghcr.io/<owner>/<repository>:master`, `:latest`, `:sha-<commit>` |
   | tag `v1.2.3` | `ghcr.io/<owner>/<repository>:1.2.3` |
   | pull request | built, not published |

   GitHub writes the image name in lower case (`ghcr.io/your-name/log-miner`). The image is for `linux/amd64` nodes.
3. If the package is private, either make it public (the package's *Package settings*) or give the pods a pull secret:

   ```bash
   kubectl -n logminer-sql01 create secret docker-registry ghcr --docker-server=ghcr.io --docker-username=<user> --docker-password=<token with read:packages>
   # then add to infra/k8s/base/deployment.yaml, under the pod's spec:   imagePullSecrets: [ { name: ghcr } ]
   ```

Without GitHub Actions, build and push it yourself (223 MB, not root, read-only root file system):

```bash
docker build -t registry.example.com/logminer:1.0 .
docker push registry.example.com/logminer:1.0
```

## 2. Prepare SQL Server

Once per instance, as a sysadmin. SQL Server 2022, on Windows or Linux, with TCP enabled and reachable from the pods.

1. **Capture.** Enable CDC on the tables to capture, then drop the CDC jobs, because logminer reads the log itself. See
   [Prepare a SQL Server](../README.md#prepare-a-sql-server) in the README.
2. **State database.** Run [`infra/sqlserver/setup-logminer-db.sql`](../infra/sqlserver/setup-logminer-db.sql). It creates `logminer`; back it up
   with the instance.
3. **Login.** Run [`infra/sqlserver/create-login.sql`](../infra/sqlserver/create-login.sql):

   ```bash
   sqlcmd -S sql01 -E -C -i infra/sqlserver/create-login.sql -v Login=logminer Password="<strong password>" Databases="tenant_%" StateDatabase=logminer SelfTest=1
   ```

   The script grants what the miner needs without `sysadmin` (measured; the script's header has the details):
   - `db_owner` in `master`, in every captured database and in the state database
   - `VIEW SERVER STATE`
   - `ALTER ANY EVENT SESSION`, so SQL Server pushes log flushes; without it the miner polls: p99 ~100 ms instead of ~7 ms
   - with `SelfTest=1`, also `dbcreator`, for the self-test after a SQL Server update

   A database created later needs the user too: run the script again (it is idempotent), or make the login `sysadmin`.
4. **Encryption.** The rows of every captured table cross this connection. By default it is encrypted (`Encrypt=Mandatory`) and the server's
   certificate is checked against the CAs the image trusts (the Ubuntu CA bundle). If that check fails:
   - The certificate is issued to another name than `LogMiner__Sql__Host` (an address, an alias, a Kubernetes Service): set
     `LogMiner__Sql__HostNameInCertificate`.
   - Nobody the image trusts signed it (self-signed, or an internal CA): pin the server's certificate itself with
     `LogMiner__Sql__ServerCertificate=/etc/logminer/files/sql01.cer`. Export it from the server, then mount it as described in step 4 of
     [4. Configure](#4-configure). This is SqlClient's `Server Certificate` option; it has not been tested here.
   - `LogMiner__Sql__TrustServerCertificate=true` accepts any certificate. Then anybody in between can read the stream, so use it only on a
     network nobody else is on.
5. **Network.** The instance's TCP keep-alive (30 s by default) decides how fast a standby takes over when the active pod's node loses its network:
   41-43 s measured. Takeover after a pod is stopped or killed takes about 1 s.

## 3. Prepare NATS

- **JetStream** must be enabled on the servers.
- Set **`sync_interval: 1s`** in the server configuration, or use a stream with 3 replicas. A NATS server acknowledges what is in memory and writes it
  to disk later (every 2 minutes by default). If its host loses power, what was acknowledged since the last sync is gone. logminer notices and
  publishes it again from SQL Server's log, which keeps changes for `ReleaseIntervalMs` (10 s) after they were acknowledged. That only works if the
  sync interval is shorter than that.
- **The stream.** logminer creates it when it is missing: `LOGMINER`, subjects `cdc.>`, file storage, 7 days, `Replicas` from the settings. It never
  changes a stream that already exists. To manage the stream yourself, set `LogMiner__Sink__NatsJetStream__ManageStream=false`; it must capture the
  subjects of `Routing:DestinationTemplate`.
- **Credentials.** Choose one way to log in, and put its secret in `secret.env`. A file (a credentials file or an NKey seed) is mounted as described
  in step 4 of [4. Configure](#4-configure).

  | Login | Settings |
  |---|---|
  | user and password | `User` (logminer.env), `Password` (secret.env) |
  | token | `Token` (secret.env) |
  | NKey | `NKeySeedFile` (mounted file) |
  | JWT (operator mode) | `CredentialsFile` (mounted `.creds` file) |

- **Permissions**, if the account restricts them. This example has stream `LOGMINER`; the publish subject is the destination template's:

  ```
  # logminer (the service)
  publish:   [ "cdc.>", "$JS.API.STREAM.INFO.LOGMINER", "$JS.API.STREAM.CREATE.LOGMINER", "$JS.API.STREAM.MSG.GET.LOGMINER" ]
  subscribe: [ "_INBOX.>" ]
  # logminer-verify (reads the stream through an ephemeral ordered consumer; publishes nothing to it)
  publish:   [ "$JS.API.STREAM.INFO.LOGMINER", "$JS.API.CONSUMER.CREATE.LOGMINER.>", "$JS.API.CONSUMER.DELETE.LOGMINER.>",
               "$JS.API.CONSUMER.MSG.NEXT.LOGMINER.>" ]
  subscribe: [ "_INBOX.>" ]
  ```

  Without `ManageStream`, `$JS.API.STREAM.CREATE.LOGMINER` is not needed.
- **TLS.** With a `tls://` URL, `TlsMode=Require`, a CA or a client certificate, the connection is encrypted. The server's certificate is checked
  against `TlsCaFile` if you set it, otherwise against the image's CAs.

## 4. Configure

The overlay [`infra/k8s/example`](../infra/k8s/example) holds the settings of one SQL Server instance. It uses
[kustomize](https://kubectl.docs.kubernetes.io/references/kustomize/), which is built into `kubectl`. Copy it once per instance:

```bash
cp -r infra/k8s/example infra/k8s/sql01
cd infra/k8s/sql01
cp secret.env.example secret.env        # git-ignored
```

1. **`kustomization.yaml` and `namespace.yaml`**: set the namespace (one per instance, e.g. `logminer-sql01`) and `images:`, your image and tag. Pin
   a tag such as `sha-1a2b3c4` or `1.2.3` in production, not `master`.
2. **`logminer.env`** holds the settings, a ConfigMap. It needs at least these:

   ```
   LogMiner__ServerName=sql01
   LogMiner__Sql__Host=sql01.example.internal
   LogMiner__Sql__User=logminer
   LogMiner__Databases__0=tenant_*
   LogMiner__Sink__NatsJetStream__Url=nats://nats.nats.svc.cluster.local:4222
   ```

   `ServerName` names the instance in the subjects, the lease and the positions. Both pods must have the same one, and it must never change. Write
   one `KEY=VALUE` per line, without quotes, with comments only on lines of their own.
3. **`secret.env`** holds the passwords, a Secret: `LogMiner__Sql__Password=...` and, for NATS, `LogMiner__Sink__NatsJetStream__Password=...` or
   `...__Token=...`. An empty value counts as not set.
4. **Files** (optional): a NATS `.creds` file, a NATS CA, or the SQL Server's certificate. Copy them into the overlay's directory (they are
   git-ignored), list them under the `logminer-files` generator in `kustomization.yaml`, and uncomment it and `patches`. They appear in the pods
   under `/etc/logminer/files/`, so point the settings there (`LogMiner__Sink__NatsJetStream__TlsCaFile=/etc/logminer/files/nats-ca.pem`).

If you'd rather make the Secret another way (sealed-secrets, External Secrets, Vault), remove `secretGenerator` and create a Secret named
`logminer-secret` in the namespace, with the same keys.

The settings can also be checked in a terminal before anything is deployed. The same environment variables work for `dotnet run --project
src/LogMiner.Service`, and the service exits with code 2 and names the setting if one is wrong.

## 5. Deploy and check

```bash
kubectl apply -k infra/k8s/sql01
kubectl -n logminer-sql01 get pods                      # two pods, both Ready (the standby is ready, too)
kubectl -n logminer-sql01 logs deploy/logminer
```

The active pod says where it connects (without passwords), that it holds the lease, and how many databases it captures:

```
SQL Server: tcp:sql01.example.internal,1433 as login 'logminer', encrypted (Mandatory), server certificate checked
sink nats-jetstream: stream LOGMINER on nats://nats.nats.svc.cluster.local:4222 as user 'logminer', TLS Auto
lease acquired: this instance is active
capturing 1200 databases
```

The other pod says `waiting for the lease logminer:sql01 (another instance may be active)`.

- **Probes and metrics** are served on port 8080: `/livez`, `/readyz` and `/metrics` (Prometheus; the pods carry `prometheus.io/*` annotations).
- **A changed setting**: edit `logminer.env` or `secret.env`, then `kubectl apply -k` again. The ConfigMap's and Secret's names change with their
  content, so the Deployment rolls.
- **A new version**: set `newTag` in `kustomization.yaml` and apply. The standby takes over while the other pod is replaced.
- **Self-test** of the SQL Server build: with `dbcreator` the service runs it by itself after every SQL Server update, before it captures. By hand:
  `kubectl -n logminer-sql01 exec deploy/logminer -- dotnet /app/logminer/logminer.dll --self-test` (exit code 0: verified).
- **Does the stream match the tables?** `kubectl -n logminer-sql01 exec deploy/logminer -- dotnet /app/verify/logminer-verify.dll`. It reads the NATS
  settings from the pod's environment and needs the read permissions shown in [3. Prepare NATS](#3-prepare-nats). It runs inside the pod's memory
  limit. For a large stream, run the same command as a Job with the same `envFrom`. See [the README](../README.md#does-the-stream-match-the-tables).

## Settings

Every setting is an environment variable. `__` separates levels, and lists are numbered (`__0`, `__1`, ...). The names follow the configuration
section `LogMiner` of [appsettings.json](../src/LogMiner.Service/appsettings.json), so `LogMiner:Sql:Host` is `LogMiner__Sql__Host`. An empty value
counts as not set. A setting that cannot work stops the service at start with exit code 2, and the log names the setting.

### SQL Server

Set the parts below (what a container is given), **or** one whole connection string in `LogMiner__ConnectionString`, not both. With neither, the
service uses shared memory and its Windows account, which only works on the database host.

| Variable | Default | |
|---|---|---|
| `LogMiner__Sql__Host` | (required) | host name or address; `host\instance` for a named instance without `Port` |
| `LogMiner__Sql__Port` | 1433 | |
| `LogMiner__Sql__User` | | SQL login; empty: the pod's own identity (Kerberos, not tested here) |
| `LogMiner__Sql__Password` | | **secret**; goes together with `User` |
| `LogMiner__Sql__Encrypt` | `Mandatory` | `Mandatory`, `Strict` (TDS 8, SQL Server 2022) or `Optional` (not encrypted unless the server insists) |
| `LogMiner__Sql__TrustServerCertificate` | `false` | `true`: any certificate is accepted (anyone in between can read the stream) |
| `LogMiner__Sql__HostNameInCertificate` | | the name the server's certificate is issued to, if not `Host` |
| `LogMiner__Sql__ServerCertificate` | | file with the server's own certificate: pinned, whoever signed it (not tested here) |
| `LogMiner__Sql__ConnectTimeoutSeconds` | 15 | |
| `LogMiner__Sql__Options` | | more SqlClient keywords, e.g. `MultiSubnetFailover=true` (not the ones above) |
| `LogMiner__ConnectionString` | | instead of all of the above: a whole SqlClient connection string (**secret** if it holds a password) |
| `LogMiner__State__Database` | `logminer` | the state database on the instance |

### NATS JetStream

All of these start with `LogMiner__Sink__NatsJetStream__`. `logminer-verify` reads the same variables.

| Variable | Default | |
|---|---|---|
| `Url` | `nats://127.0.0.1:4222` | one server, or several separated by commas; `nats://`, `tls://`, `ws://`, `wss://` |
| `Stream` | `LOGMINER` | |
| `User` / `Password` | | user and password (**secret**) |
| `Token` | | **secret** |
| `NKeySeedFile` | | file with a user's NKey seed |
| `CredentialsFile` | | `.creds` file (JWT and NKey seed, operator mode) |
| `TlsMode` | `Auto` | `Auto` (TLS when the URL, a CA or a certificate asks for it, or the server does), `Require`, `Implicit`, `Prefer`, `Disable` |
| `TlsCaFile` | | PEM file of the CA that signed the server's certificate |
| `TlsCertFile` / `TlsKeyFile` | | client certificate and key (PEM), if the server checks clients by certificate |
| `TlsInsecureSkipVerify` | `false` | `true`: any server certificate is accepted (tests only) |
| `ManageStream` | `true` | create the stream when it is missing (never changes one that exists) |
| `Subjects__0` | `cdc.>` | subjects of a stream it creates |
| `Replicas` | 1 | replicas of a stream it creates |
| `MaxAgeDays` | 7 | age limit of a stream it creates |
| `DuplicateWindowMinutes` | 10 | de-duplication window of a stream it creates; well above the longest restart |
| `AckTimeoutSeconds` | 10 | how long a publish may wait for its acknowledgement |

Only one way to log in may be set. A file setting is checked at start: a missing file means the Secret is not mounted.

### Capture

| Variable | Default | |
|---|---|---|
| `LogMiner__ServerName` | (required off Windows) | the instance's name in subjects, lease and positions; the same in both pods, never changed |
| `LogMiner__Databases__0`, `__1`, ... | every CDC-enabled database | names to capture, `*` and `?` as wildcards |
| `LogMiner__ExcludeDatabases__0`, ... | | names not to capture |
| `LogMiner__StartPosition` | `Auto` | a database without a position: `Auto` (where SQL Server's replication state says, else the end), `Earliest`, `Latest` |
| `LogMiner__OnGap` | `Resync` | when changes may be missing (log released by someone else, a restore): `Resync` loads the tables again, `Continue` goes on; always recorded in `logminer.dbo.gaps` |
| `LogMiner__SelfTest` | `Require` | verify a new SQL Server build in a scratch database before capturing: `Require`, `Warn`, `Off` |
| `LogMiner__Workers` | 8 | databases read at the same time |
| `LogMiner__MaxBufferedMb` | 1024 | row images of open transactions per database; a larger transaction stops its database (the log is kept) |
| `LogMiner__MaxRowsPerRead` | 20000 | |
| `LogMiner__CheckpointIntervalMs` | 1000 | how often positions are saved |
| `LogMiner__ReleaseIntervalMs` | 10000 | how long SQL Server keeps a change after it was acknowledged; longer than NATS's `sync_interval` |
| `LogMiner__ReleaseLog` | `true` | let SQL Server truncate what was published (`sp_repldone`) |
| `LogMiner__MaxHeldLogMb` | 64 | log a quiet database may hold as a bookmark |
| `LogMiner__SweepIntervalSeconds` | 30 | every database is read at least this often, pushed or not (a safety net) |
| `LogMiner__Routing__DestinationTemplate` | `cdc.{server}.{db}.{schema}.{table}` | the subject of an event |
| `LogMiner__Routing__PartitionKey` | `Database` | order per `Database`, `Table` or `RowKey` |

### Push of log flushes (the doorbell)

| Variable | Default | |
|---|---|---|
| `LogMiner__Signal__Push` | `true` | SQL Server pushes log flushes through an extended-event session (needs `ALTER ANY EVENT SESSION`); otherwise it polls |
| `LogMiner__Signal__EventSession` | `logminer_doorbell_<ServerName>` | |
| `LogMiner__Signal__DispatchIntervalMs` | 4 | |
| `LogMiner__Signal__AuditIntervalMs` | 1000 | |
| `LogMiner__Signal__FullPollIntervalMs` | 100 | |
| `LogMiner__Signal__HotPollIntervalMs` | 5 | |
| `LogMiner__Signal__HotWindowSeconds` | 10 | |

### The process

| Variable | Default | |
|---|---|---|
| `LogMiner__HealthPort` | 8080 in the image | `/livez`, `/readyz`, `/metrics`; 0: off |
| `LogMiner__HealthLogIntervalSeconds` | 30 | a health line in the log |
| `Logging__LogLevel__Default` | `Information` | |
| `Logging__Console__FormatterName` | `simple` | `json` for log collectors |

The spool (`LogMiner__Sink__Spool__*`) and file positions (`LogMiner__State__Type=File`) need a durable disk on the host. They are not meant for a
pod.

## When something is wrong

| The log says | |
|---|---|
| `configuration: no SQL Server is configured` | set `LogMiner__Sql__Host` (or `LogMiner__ConnectionString`) |
| `configuration: ServerName is not set` | set `LogMiner__ServerName`, the same in both pods |
| `configuration: ... go together` | a user without a password (or the other way round): is `secret.env` filled in and applied? |
| `configuration: ...: the file /etc/logminer/files/... is not there` | the `logminer-files` Secret is not mounted (`patches` in `kustomization.yaml`) |
| `Login failed for user 'logminer'` | wrong password, or the login does not exist (`create-login.sql`) |
| `The certificate chain was issued by an authority that is not trusted`, or the name does not match | see the encryption step of [2. Prepare SQL Server](#2-prepare-sql-server): `HostNameInCertificate`, `ServerCertificate` |
| `Authorization Violation` (NATS) | wrong NATS user, password, token or credentials |
| `Permissions Violation for Publish to ...` (NATS server log) | the account's permissions: see [3. Prepare NATS](#3-prepare-nats) |
| `self-test FAILED`, exit code 4 | this SQL Server build changed something logminer relies on. It does not capture, and the log is kept. Open an issue with the log |
| `NO LOG IS RELEASED` | the stream does not hold what it acknowledged; see [the README](../README.md#missing-changes-are-never-silent) |

A pod in `CrashLoopBackOff` is usually a configuration problem (exit code 2): `kubectl -n logminer-sql01 logs deploy/logminer --previous`.
