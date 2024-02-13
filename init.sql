-- DATABASE
ALTER DATABASE RINHA SET SYNCHRONOUS_COMMIT=OFF;

SET STATEMENT_TIMEOUT = 0;
SET LOCK_TIMEOUT = 0;
SET IDLE_IN_TRANSACTION_SESSION_TIMEOUT = 0;
SET CLIENT_ENCODING = 'UTF8';
SET STANDARD_CONFORMING_STRINGS = ON;
SET CHECK_FUNCTION_BODIES = FALSE;
SET XMLOPTION = CONTENT;
SET CLIENT_MIN_MESSAGES = WARNING;
SET ROW_SECURITY = OFF;
SET DEFAULT_TABLESPACE = '';
SET DEFAULT_TABLE_ACCESS_METHOD = HEAP;

-- CLIENTE
CREATE UNLOGGED TABLE IF NOT EXISTS CLIENTE (
     ID SMALLINT,
     LIMITE INT NOT NULL,
     SALDO INT DEFAULT 0
);
ALTER TABLE CLIENTE DISABLE TRIGGER ALL;
CREATE INDEX PK_CLIENTE_IDX ON CLIENTE (ID) INCLUDE (SALDO) WITH (FILLFACTOR = 100);
CLUSTER CLIENTE USING PK_CLIENTE_IDX;

INSERT INTO CLIENTE (ID, LIMITE) VALUES (1, 1000*100),(2, 800*100),(3, 10000*100),(4, 100000*100),(5, 5000*100);

-- TRANSACAO
CREATE UNLOGGED TABLE IF NOT EXISTS TRANSACAO (
    CLIENTE_ID SMALLINT NOT NULL,
    VALOR INT NOT NULL,
    TIPO CHAR(1) NOT NULL,
    DESCRICAO VARCHAR(10) NOT NULL,
    REALIZADA_EM BIGINT default (extract(epoch from now()) * 1000)
) WITH (FILLFACTOR = 70, AUTOVACUUM_ENABLED = TRUE);
ALTER TABLE TRANSACAO DISABLE TRIGGER ALL;
CREATE INDEX CLIENTE_IDX ON TRANSACAO (CLIENTE_ID) WITH (FILLFACTOR = 70) ;
CREATE INDEX REALIZADA_EM_IDX ON TRANSACAO (REALIZADA_EM DESC) WITH (FILLFACTOR = 70) ;
CLUSTER TRANSACAO USING CLIENTE_IDX;

CREATE FUNCTION D(
    CLIENTE_ID SMALLINT,
    VALOR INT,
    DESCRICAO TEXT,
    P_LIMITE INT,
    INOUT NOVO_SALDO INT DEFAULT NULL)
    LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(CLIENTE_ID);
    UPDATE CLIENTE SET SALDO = SALDO - VALOR
    WHERE ID = CLIENTE_ID AND saldo - VALOR >= - P_LIMITE
    RETURNING SALDO INTO NOVO_SALDO;

    IF NOVO_SALDO IS NULL THEN RETURN; END IF;

    INSERT INTO TRANSACAO (CLIENTE_ID, VALOR, TIPO, DESCRICAO)
    VALUES(CLIENTE_ID, VALOR, 'd', DESCRICAO);
END;
$$;

CREATE FUNCTION C(
    CLIENTE_ID SMALLINT,
    VALOR INT,
    DESCRICAO TEXT,
    P_LIMITE INT,
    INOUT NOVO_SALDO INT DEFAULT NULL)
    LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(CLIENTE_ID);
    UPDATE CLIENTE SET SALDO = SALDO + VALOR
    WHERE ID = CLIENTE_ID AND saldo + VALOR >= - P_LIMITE
    RETURNING SALDO INTO NOVO_SALDO;

    IF NOVO_SALDO IS NULL THEN RETURN; END IF;

    INSERT INTO TRANSACAO (CLIENTE_ID, VALOR, TIPO, DESCRICAO)
    VALUES(CLIENTE_ID, VALOR, 'c', DESCRICAO);
END;
$$;




