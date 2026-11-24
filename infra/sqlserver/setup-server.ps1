<#
  Post-install server configuration for the logminer test host (idempotent, runs as a sysadmin login):
   - backup folder the SQL Server service can write and this user can read
   - database master key + TDE server certificate (backed up WITH PRIVATE KEY)
   - small model database so 1,000 databases stay cheap
  Secrets are generated into ~/.logminer (user-only ACL) and never printed.
#>
param(
  [string] $Server = 'localhost',
  [string] $SecretDir = "$env:USERPROFILE\.logminer",
  [string] $BackupDir = 'C:\SQLBackup',
  [string] $Sqlcmd = "$env:USERPROFILE\tools\sqlcmd\sqlcmd.exe"
)
$ErrorActionPreference = 'Stop'

function Get-OrCreateSecret([string] $name) {
  $path = Join-Path $SecretDir "$name.secret"
  if (-not (Test-Path $path)) {
    $chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789'.ToCharArray()
    $bytes = [byte[]]::new(32); $rng = [Security.Cryptography.RandomNumberGenerator]::Create(); $rng.GetBytes($bytes); $rng.Dispose()
    Set-Content -Path $path -Value ("Aa9-" + (-join ($bytes | ForEach-Object { $chars[$_ % $chars.Length] }))) -NoNewline -Encoding ascii
  }
  (Get-Content $path -Raw).Trim()
}

if (-not (Test-Path $BackupDir)) { New-Item -ItemType Directory $BackupDir | Out-Null }
icacls $BackupDir /inheritance:r /grant:r "SYSTEM:(OI)(CI)F" "BUILTIN\Administrators:(OI)(CI)F" "$($env:USERDOMAIN)\$($env:USERNAME):(OI)(CI)F" "NT Service\MSSQLSERVER:(OI)(CI)M" | Out-Null

$dmk  = Get-OrCreateSecret 'dmk'
$cert = Get-OrCreateSecret 'tde-cert-backup'

$tsql = @"
SET NOCOUNT ON;
IF NOT EXISTS (SELECT 1 FROM master.sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
  EXEC ('USE master; CREATE MASTER KEY ENCRYPTION BY PASSWORD = ''$dmk'';');
IF NOT EXISTS (SELECT 1 FROM master.sys.certificates WHERE name = 'LogMinerTdeCert')
BEGIN
  EXEC ('USE master; CREATE CERTIFICATE LogMinerTdeCert WITH SUBJECT = ''logminer TDE server certificate'', EXPIRY_DATE = ''2036-01-01'';');
  EXEC ('USE master; BACKUP CERTIFICATE LogMinerTdeCert TO FILE = ''$BackupDir\LogMinerTdeCert.cer''
         WITH PRIVATE KEY (FILE = ''$BackupDir\LogMinerTdeCert.pvk'', ENCRYPTION BY PASSWORD = ''$cert'');');
END
-- CDC/replication refuse LOB values > 64 KB on tracked tables unless this is unlimited (error 7139).
EXEC sp_configure 'max text repl size', -1; RECONFIGURE;
-- Small model: 1,000 databases x (4 MB data + 4 MB log). Log grows in 64 MB steps (sane VLF count).
EXEC ('USE model; DBCC SHRINKFILE (modeldev, 4) WITH NO_INFOMSGS; DBCC SHRINKFILE (modellog, 4) WITH NO_INFOMSGS;');
ALTER DATABASE model MODIFY FILE (NAME = modeldev, FILEGROWTH = 64MB);
ALTER DATABASE model MODIFY FILE (NAME = modellog, FILEGROWTH = 64MB);
ALTER DATABASE model SET RECOVERY FULL;
SELECT name, size * 8 / 1024 AS size_mb, growth * 8 / 1024 AS growth_mb FROM model.sys.database_files;
SELECT name, thumbprint, expiry_date FROM master.sys.certificates WHERE name = 'LogMinerTdeCert';
"@
$tmp = Join-Path $SecretDir 'setup-server.sql'   # contains secrets: lives in the user-only folder, deleted after use
Set-Content -Path $tmp -Value $tsql -Encoding utf8
try { & $Sqlcmd -S $Server -E -C -b -i $tmp; if ($LASTEXITCODE -ne 0) { throw "sqlcmd failed: $LASTEXITCODE" } }
finally { Remove-Item $tmp -ErrorAction SilentlyContinue }
