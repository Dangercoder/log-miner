# Runs elevated (one UAC prompt): PDH doorbell benchmark + TDE log-file readability probe.
param([string] $Out = "$env:USERPROFILE\.logminer\elevated-probe.txt")
$ErrorActionPreference = 'Continue'
$repo = Split-Path $PSScriptRoot -Parent
& {
  "=== PDH: Repl. Pending Xacts"
  dotnet run --project "$repo\tools\LogMiner.Lab" -c Release --no-build -- pdh --counter "Repl. Pending Xacts" --n 2000 2>&1
  "=== PDH: Log Flushes/sec"
  dotnet run --project "$repo\tools\LogMiner.Lab" -c Release --no-build -- pdh --counter "Log Flushes/sec" --n 500 2>&1

  "=== TDE .ldf probe (lab_a: TDE on; its log holds transaction names like 'y01_add_col')"
  $ldf = Get-ChildItem 'C:\Program Files\Microsoft SQL Server\MSSQL16.MSSQLSERVER\MSSQL\DATA' -Filter 'lab_a_log.ldf' | Select-Object -First 1
  try {
    $fs = [IO.File]::Open($ldf.FullName, [IO.FileMode]::Open, [IO.FileAccess]::Read, [IO.FileShare]::ReadWrite)
    "opened $($ldf.FullName) for shared read while the engine runs: length $($fs.Length)"
    $buf = New-Object byte[] $fs.Length
    $read = 0; while ($read -lt $buf.Length) { $n = $fs.Read($buf, $read, $buf.Length - $read); if ($n -le 0) { break }; $read += $n }
    $fs.Dispose()
    $needle = [Text.Encoding]::Unicode.GetBytes('y01_add_col')
    $hex = [BitConverter]::ToString($buf, 0, $buf.Length).Replace('-', '')
    $found = $hex.IndexOf([BitConverter]::ToString($needle).Replace('-', ''))
    "plaintext transaction name found in file: $($found -ge 0)"
    # byte histogram entropy of a 64 KB window from the middle of the file
    $start = [int]($buf.Length / 2); $win = 65536
    $counts = New-Object int[] 256; for ($i = $start; $i -lt $start + $win; $i++) { $counts[$buf[$i]]++ }
    $h = 0.0; foreach ($c in $counts) { if ($c -gt 0) { $p = $c / $win; $h -= $p * [Math]::Log($p, 2) } }
    "entropy of 64 KB mid-file window: {0:N3} bits/byte (8.0 = random/encrypted)" -f $h
    "first 64 bytes at 8 KB: " + [BitConverter]::ToString($buf, 8192, 64)
  } catch { "ldf open failed: $($_.Exception.Message)" }
} *> $Out
