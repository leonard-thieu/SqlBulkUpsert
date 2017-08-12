[CmdletBinding()]
param(
    [string]$Database = 'SqlBulkUpsertTestDb',
    [string]$Instance = 'localhost'
)

$mdfFileName = "$PSScriptRoot\$Database.mdf"
$ldfFileName = "$PSScriptRoot\$Database`_log.ldf"
sqlcmd -S "$Instance" -Q "USE [master]; CREATE DATABASE [$Database] ON (FILENAME = '$mdfFileName'), (FILENAME = '$ldfFileName') FOR ATTACH;"
if ($LASTEXITCODE -ne 0) { throw "Execution failed with exit code $LASTEXITCODE" }