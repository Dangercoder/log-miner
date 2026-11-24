<#
  Installs SQL Server 2022 Developer (engine only) unattended, slipstreaming a local CU.
  Must run elevated. Usage:
    Start-Process powershell -Verb RunAs -Wait -ArgumentList '-ExecutionPolicy Bypass -File install-sqlserver.ps1 -SysAdmin DOMAIN\user'

  Media (downloaded beforehand, both from download.microsoft.com):
    <WorkDir>\SQLServer2022-x64-ENU-Dev.iso
    <WorkDir>\cu\SQLServer2022-KB*-x64.exe
#>
param(
  [Parameter(Mandatory)] [string] $SysAdmin,
  [string] $WorkDir = "$env:USERPROFILE\Downloads\sql2022-setup",
  [string] $SecretFile = "$env:USERPROFILE\.logminer\sa.secret"
)
$ErrorActionPreference = 'Stop'
$log = Join-Path $WorkDir 'install.log'
Start-Transcript -Path $log -Force | Out-Null
$iso = Join-Path $WorkDir 'SQLServer2022-x64-ENU-Dev.iso'
$mounted = $false
try {
  $isAdmin = ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
  if (-not $isAdmin) { throw 'install-sqlserver.ps1 must run elevated' }

  $img = Mount-DiskImage -ImagePath $iso -PassThru
  $mounted = $true
  $drive = ($img | Get-Volume).DriveLetter
  $setup = "${drive}:\setup.exe"
  $sig = Get-AuthenticodeSignature $setup
  if ($sig.Status -ne 'Valid' -or $sig.SignerCertificate.Subject -notmatch 'O=Microsoft Corporation') { throw "setup.exe signature not trusted: $($sig.Status) $($sig.SignerCertificate.Subject)" }
  Get-ChildItem (Join-Path $WorkDir 'cu') -Filter '*.exe' | ForEach-Object {
    $s = Get-AuthenticodeSignature $_.FullName
    if ($s.Status -ne 'Valid' -or $s.SignerCertificate.Subject -notmatch 'O=Microsoft Corporation') { throw "CU signature not trusted: $($_.Name) $($s.Status)" }
  }

  Write-Host "running $setup (engine + slipstreamed CU)"
  $ini = Join-Path $PSScriptRoot 'ConfigurationFile.ini'
  $sapwd = Get-Content $SecretFile -Raw
  $setupArgs = @(
    "/CONFIGURATIONFILE=`"$ini`"",
    "/SQLSYSADMINACCOUNTS=`"$SysAdmin`"",
    "/SAPWD=`"$sapwd`"",
    '/UPDATEENABLED=True',
    "/UPDATESOURCE=`"$(Join-Path $WorkDir 'cu')`"",
    '/IACCEPTSQLSERVERLICENSETERMS'
  )
  $p = Start-Process $setup -ArgumentList $setupArgs -Wait -PassThru
  Write-Host "setup exit code: $($p.ExitCode)"
  $summary = "$env:ProgramFiles\Microsoft SQL Server\160\Setup Bootstrap\Log\Summary.txt"
  if (Test-Path $summary) { Get-Content $summary -TotalCount 60 }
  if ($p.ExitCode -ne 0 -and $p.ExitCode -ne 3010) { throw "setup failed: $($p.ExitCode)" }
}
catch { Write-Host "ERROR: $_"; throw }
finally {
  if ($mounted) { Dismount-DiskImage -ImagePath $iso | Out-Null }
  Stop-Transcript | Out-Null
}
