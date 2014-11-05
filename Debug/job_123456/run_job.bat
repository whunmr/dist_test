CD /D %~dp0
set GTEST_TOTAL_SHARDS=%1
set GTEST_SHARD_INDEX=%2
mkdir __dist
dummy_tests.exe 2>&1 | %CD%\..\wtee.exe __dist\run.log
