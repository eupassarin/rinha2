-- DATABASE
ALTER DATABASE RINHA SET SYNCHRONOUS_COMMIT=OFF;
SET CLIENT_MIN_MESSAGES = WARNING;
SET ROW_SECURITY = OFF;

-- CLIENTE
CREATE UNLOGGED TABLE IF NOT EXISTS CLIENTE (
     ID SMALLINT,
     LIMITE INT NOT NULL,
     SALDO INT DEFAULT 0
);
CREATE INDEX PK_CLIENTE_IDX ON CLIENTE (ID) INCLUDE (SALDO);
CLUSTER CLIENTE USING PK_CLIENTE_IDX;

INSERT INTO CLIENTE (ID, LIMITE) VALUES (1, 1000*100),(2, 800*100),(3, 10000*100),(4, 100000*100),(5, 5000*100);

-- TRANSACAO
CREATE UNLOGGED TABLE IF NOT EXISTS TRANSACAO (
    CLIENTE_ID SMALLINT NOT NULL,
    VALOR INT NOT NULL,
    TIPO CHAR(1) NOT NULL,
    DESCRICAO VARCHAR(10) NOT NULL,
    REALIZADA_EM BIGINT default (extract(epoch from now()) * 1000)
);

CREATE INDEX CLIENTE_IDX ON TRANSACAO (CLIENTE_ID);
CREATE INDEX REALIZADA_EM_IDX ON TRANSACAO (REALIZADA_EM DESC);
CLUSTER TRANSACAO USING CLIENTE_IDX;

CREATE FUNCTION D(
    CLIENTE_ID SMALLINT,
    VALOR INT,
    DESCRICAO TEXT,
    P_LIMITE INT,
    OUT NOVO_SALDO INT)
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
    OUT NOVO_SALDO INT)
    LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock(CLIENTE_ID);
    UPDATE CLIENTE SET SALDO = SALDO + VALOR
    WHERE ID = CLIENTE_ID
    RETURNING SALDO INTO NOVO_SALDO;

    INSERT INTO TRANSACAO (CLIENTE_ID, VALOR, TIPO, DESCRICAO)
    VALUES(CLIENTE_ID, VALOR, 'c', DESCRICAO);
END;
$$;




