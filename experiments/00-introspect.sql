SELECT @@VERSION AS version, SERVERPROPERTY('ProductUpdateLevel') AS cu, SERVERPROPERTY('Edition') AS edition;
SELECT name, type_desc FROM sys.all_objects WHERE name LIKE '%dblog%' ORDER BY name;
SELECT OBJECT_NAME(object_id) AS fn, parameter_id, name, TYPE_NAME(user_type_id) AS type, max_length
FROM sys.all_parameters
WHERE object_id IN (OBJECT_ID('sys.fn_dblog'), OBJECT_ID('sys.fn_full_dblog'), OBJECT_ID('sys.fn_dump_dblog'), OBJECT_ID('sys.fn_dblog_xtp'))
ORDER BY fn, parameter_id;
SELECT column_id, name, TYPE_NAME(user_type_id) AS type, max_length
FROM sys.all_columns WHERE object_id = OBJECT_ID('sys.fn_dblog') ORDER BY column_id;
SELECT OBJECT_DEFINITION(OBJECT_ID('sys.fn_dblog')) AS fn_dblog_def;
