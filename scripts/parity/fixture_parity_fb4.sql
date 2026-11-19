-- Firebird 4+ part of the firebird_query parity fixture (see fixture_parity.sql).
-- Types a Firebird 3 server doesn't have: INT128-backed NUMERIC(38,x), DECFLOAT,
-- TIMESTAMP / TIME WITH TIME ZONE. parity.py --create loads it only on Firebird 4+.

SET SQL DIALECT 3;
SET AUTODDL ON;

CREATE TABLE PAR_FB4 (
    ID      INTEGER NOT NULL PRIMARY KEY,
    BIG     NUMERIC(38,4),
    DF      DECFLOAT(34),
    TS_TZ   TIMESTAMP WITH TIME ZONE,
    T_TZ    TIME WITH TIME ZONE
);

SET TERM ^ ;
EXECUTE BLOCK AS
DECLARE i INTEGER = 1;
BEGIN
    WHILE (i <= 10000) DO
    BEGIN
        INSERT INTO PAR_FB4 (ID, BIG, DF, TS_TZ, T_TZ)
        VALUES (:i,
                IIF(MOD(:i, 6) = 0, NULL, CAST(:i AS NUMERIC(38,4)) * 1000000000000000 + CAST(:i AS NUMERIC(18,4)) / 7),
                IIF(MOD(:i, 5) = 0, NULL, CAST(:i AS DECFLOAT(34)) / 3),
                DATEADD(:i SECOND TO TIMESTAMP '2026-01-01 12:00:00 America/Sao_Paulo'),
                TIME '10:00:00 UTC');
        i = i + 1;
    END
END^
SET TERM ; ^

COMMIT;
