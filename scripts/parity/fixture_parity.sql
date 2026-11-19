-- Synthetic fixture for the firebird_query parity / benchmark suite (scripts/parity/).
-- Firebird 3/4/5, dialect 3. Load into an empty UTF8 database with isql:
--   isql -user SYSDBA -password masterkey -i fixture_parity.sql <host:/path/parity.fdb>
-- (parity.py --create does both steps). Firebird 4+ types live in fixture_parity_fb4.sql.
--
-- Shapes it reproduces (each one is an effect the suite measures):
--   * a header/detail pair (PAR_ORDER 200k / PAR_ORDER_ITEM 1M) filtered through the
--     header: a DuckDB join over ATTACHed tables reads the detail table in full;
--   * COMPUTED BY columns, one of them a correlated subquery (re-evaluated per row);
--   * a selectable stored procedure doing per-row work (PAR_SP_PRODUCT_SALES);
--   * text BLOBs, some with an embedded NUL byte;
--   * CHAR(n) padding;
--   * SUM(NUMERIC(9,3) * NUMERIC(18,4)) -- NUMERIC(38,7) (INT128) on Firebird 4+.
-- Values are deterministic (MOD arithmetic), so every run builds the same data.

SET SQL DIALECT 3;
SET AUTODDL ON;

CREATE TABLE PAR_CUSTOMER (
    ID        INTEGER NOT NULL PRIMARY KEY,
    NAME      VARCHAR(60) NOT NULL,
    CITY      VARCHAR(30),
    UF        CHAR(2),
    NOTES     BLOB SUB_TYPE TEXT
);

CREATE TABLE PAR_PRODUCT (
    ID        INTEGER NOT NULL PRIMARY KEY,
    DESCR     VARCHAR(80) NOT NULL,
    UNIT      CHAR(6) NOT NULL,
    PRICE     NUMERIC(18,4) NOT NULL,
    COST      NUMERIC(18,4),
    MARGIN    COMPUTED BY (PRICE - COALESCE(COST, 0))
);

CREATE TABLE PAR_ORDER (
    ID            INTEGER NOT NULL PRIMARY KEY,
    CUSTOMER_ID   INTEGER NOT NULL REFERENCES PAR_CUSTOMER,
    ISSUED        DATE NOT NULL,
    STATUS        CHAR(1) NOT NULL,
    FREIGHT_TOTAL NUMERIC(18,4)
);
CREATE INDEX PAR_ORDER_ISSUED ON PAR_ORDER (ISSUED);

CREATE TABLE PAR_ORDER_ITEM (
    ID          INTEGER NOT NULL PRIMARY KEY,
    ORDER_ID    INTEGER NOT NULL REFERENCES PAR_ORDER,
    PRODUCT_ID  INTEGER NOT NULL REFERENCES PAR_PRODUCT,
    QTY         NUMERIC(9,3) NOT NULL,
    PRICE       NUMERIC(18,4) NOT NULL,
    DISCOUNT    NUMERIC(18,4),
    -- correlated: re-evaluated for every row that reads it
    FREIGHT     COMPUTED BY ((SELECT o.FREIGHT_TOTAL FROM PAR_ORDER o WHERE o.ID = ORDER_ID) / 5)
);

SET TERM ^ ;

CREATE PROCEDURE PAR_SP_PRODUCT_SALES
RETURNS (PRODUCT_ID INTEGER, SOLD NUMERIC(18,3), REVENUE NUMERIC(18,4), LAST_SOLD DATE)
AS
BEGIN
    FOR SELECT ID FROM PAR_PRODUCT ORDER BY ID INTO :PRODUCT_ID DO
    BEGIN
        SELECT SUM(QTY), CAST(SUM(QTY * PRICE) AS NUMERIC(18,4))
          FROM PAR_ORDER_ITEM WHERE PRODUCT_ID = :PRODUCT_ID
          INTO :SOLD, :REVENUE;
        SELECT MAX(o.ISSUED)
          FROM PAR_ORDER_ITEM i JOIN PAR_ORDER o ON o.ID = i.ORDER_ID
         WHERE i.PRODUCT_ID = :PRODUCT_ID
          INTO :LAST_SOLD;
        SUSPEND;
    END
END^

EXECUTE BLOCK AS
DECLARE i INTEGER = 1;
BEGIN
    WHILE (i <= 50000) DO
    BEGIN
        INSERT INTO PAR_CUSTOMER (ID, NAME, CITY, UF, NOTES)
        VALUES (:i, 'Customer ' || :i,
                'City ' || MOD(:i, 400),
                IIF(MOD(:i, 3) = 0, 'SP', IIF(MOD(:i, 3) = 1, 'RJ', 'MG')),
                IIF(MOD(:i, 997) = 0, 'note ' || :i || ASCII_CHAR(0) || ' after the NUL',
                    IIF(MOD(:i, 3) = 0, NULL, 'note ' || :i)));
        i = i + 1;
    END
END^

EXECUTE BLOCK AS
DECLARE i INTEGER = 1;
BEGIN
    WHILE (i <= 20000) DO
    BEGIN
        INSERT INTO PAR_PRODUCT (ID, DESCR, UNIT, PRICE, COST)
        VALUES (:i, 'Product ' || :i,
                IIF(MOD(:i, 2) = 0, 'UND', 'CX'),
                MOD(:i * 37, 5000) / 10.0 + 1,
                IIF(MOD(:i, 5) = 0, NULL, MOD(:i * 37, 5000) / 20.0));
        i = i + 1;
    END
END^

EXECUTE BLOCK AS
DECLARE i INTEGER = 1;
BEGIN
    WHILE (i <= 200000) DO
    BEGIN
        INSERT INTO PAR_ORDER (ID, CUSTOMER_ID, ISSUED, STATUS, FREIGHT_TOTAL)
        VALUES (:i, MOD(:i * 31, 50000) + 1,
                DATEADD(MOD(:i * 37, 1826) DAY TO DATE '2021-01-01'),
                IIF(MOD(:i, 10) = 0, 'C', 'F'),
                IIF(MOD(:i, 7) = 0, NULL, MOD(:i, 100) * 1.5));
        i = i + 1;
    END
END^

EXECUTE BLOCK AS
DECLARE i INTEGER = 1;
BEGIN
    WHILE (i <= 1000000) DO
    BEGIN
        INSERT INTO PAR_ORDER_ITEM (ID, ORDER_ID, PRODUCT_ID, QTY, PRICE, DISCOUNT)
        VALUES (:i, (:i - 1) / 5 + 1, MOD(:i * 7919, 20000) + 1,
                MOD(:i, 13) + 0.5,
                MOD(:i * 13, 3000) / 10.0 + 0.99,
                IIF(MOD(:i, 4) = 0, NULL, MOD(:i, 50) / 10.0));
        i = i + 1;
    END
END^

SET TERM ; ^

COMMIT;
