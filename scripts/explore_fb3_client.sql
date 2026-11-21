.echo on
.bail off
SELECT client_version, engine_version FROM firebird_health('rw');
SELECT * FROM firebird_execute('rw', 'DELETE FROM RW_TARGET WHERE ID >= 9300 AND ID < 9400');
SELECT * FROM firebird_execute('rw', 'INSERT INTO RW_TARGET (ID, C_NUM18_4) VALUES (?, ?)', source := $$SELECT * FROM (VALUES (9300, 1.5::DECIMAL(38,10)), (9301, -123456789.1234::DECIMAL(38,10))) t$$);
SELECT ID, C_NUM18_4 FROM rw.main.RW_TARGET WHERE ID IN (9300, 9301) ORDER BY ID;
SELECT * FROM firebird_execute('rw', 'INSERT INTO RW_TARGET (ID, C_NUM18_4) VALUES (?, ?)', source := $$SELECT 9302, 1.12345678901234567891::DECIMAL(38,20)$$);
SELECT ID, C_NUM18_4 FROM rw.main.RW_TARGET WHERE ID = 9302;
SELECT * FROM firebird_execute('rw', 'INSERT INTO RW_TARGET (ID, C_NUM18_4) VALUES (9303, 1.12345678901234567891)');
SELECT ID, C_NUM18_4 FROM rw.main.RW_TARGET WHERE ID = 9303;
SET firebird_write_batch_size = 1000;
INSERT INTO rw.main.RW_TARGET (ID, C_NUM18_4) SELECT 9310 + i, i / 4 FROM range(5) t(i);
SELECT write_mode, rows_written, rows_affected, error_message FROM firebird_last_query();
SELECT * FROM firebird_execute('rw', 'INSERT INTO RW_TARGET (ID, C_NUM18_4) VALUES (?, ?)', source := 'SELECT 9320 + i, i / 4 FROM range(5) t(i)');
SELECT write_mode, rows_written, rows_affected, error_message FROM firebird_last_query();
SELECT count(*), sum(C_NUM18_4) FROM rw.main.RW_TARGET WHERE ID BETWEEN 9310 AND 9324;
SELECT * FROM firebird_execute('rw', 'INSERT INTO RW_TARGET (ID, C_NUM18_4) VALUES (?, ?)', source := 'SELECT 9330, 12345678901234.5678::DECIMAL(18,4)');
SELECT ID, C_NUM18_4 FROM rw.main.RW_TARGET WHERE ID = 9330;
SELECT * FROM firebird_execute('rw', 'DELETE FROM RW_TARGET WHERE ID >= 9300 AND ID < 9400');
