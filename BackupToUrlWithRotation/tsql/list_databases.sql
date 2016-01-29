SELECT name, database_id, is_read_only, state_desc, recovery_model_desc FROM sys.databases
WHERE database_id <> 2 -- exclude TempDB