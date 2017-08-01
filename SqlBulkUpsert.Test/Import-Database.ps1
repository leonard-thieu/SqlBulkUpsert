$startPath = "$($env:appveyor_build_folder)\SqlBulkUpsert.Test"
$sqlInstance = "(local)\SQL2014"
$dbName = "SqlBulkUpsertTestDb"

# attach mdf to local instance
$mdfFile = join-path $startPath "SqlBulkUpsertTestDb.mdf"
$ldfFile = join-path $startPath "SqlBulkUpsertTestDb_log.ldf"
sqlcmd -S "$sqlInstance" -Q "Use [master]; CREATE DATABASE [$dbName] ON (FILENAME = '$mdfFile'),(FILENAME = '$ldfFile') for ATTACH"