-- CLIENTE
CREATE UNLOGGED TABLE IF NOT EXISTS CLIENTE (
     ID SERIAL PRIMARY KEY,
     LIMITE INT NOT NULL,
     SALDO INT DEFAULT 0
) WITH (FILLFACTOR = 70, AUTOVACUUM_ENABLED = FALSE);
ALTER TABLE CLIENTE DISABLE TRIGGER ALL;

INSERT INTO CLIENTE (ID, LIMITE) VALUES
 (1, 1000*100),(2, 800*100),(3, 10000*100),(4, 100000*100),(5, 5000*100);

-- TRANSACAO
CREATE UNLOGGED TABLE IF NOT EXISTS TRANSACAO (
    CLIENTE_ID INT NOT NULL,
    VALOR INT NOT NULL,
    TIPO CHAR(1) NOT NULL,
    DESCRICAO VARCHAR(10) NOT NULL,
    REALIZADA_EM TIMESTAMP DEFAULT NOW(),

     CONSTRAINT fk_clientes_transacoes_id FOREIGN KEY (CLIENTE_ID)
         REFERENCES CLIENTE(ID) DEFERRABLE INITIALLY DEFERRED

) WITH (FILLFACTOR = 70, AUTOVACUUM_ENABLED = FALSE);

ALTER TABLE TRANSACAO DISABLE TRIGGER ALL;
CREATE INDEX REALIZADA_EM_IDX ON TRANSACAO (REALIZADA_EM DESC) WITH (FILLFACTOR = 70) ;
CLUSTER TRANSACAO USING REALIZADA_EM_IDX;

--PROCEDURE
CREATE PROCEDURE T(
    CLIENTE_ID INT,
    VALOR INT,
    TIPO TEXT,
    DESCRICAO TEXT,
    INOUT NOVO_SALDO INT DEFAULT NULL,
    INOUT RET_LIMITE INT DEFAULT NULL)
LANGUAGE plpgsql
AS $$
BEGIN
  UPDATE CLIENTE SET SALDO = SALDO + VALOR
  WHERE ID = CLIENTE_ID AND saldo + VALOR >= - LIMITE
  RETURNING SALDO, LIMITE INTO NOVO_SALDO, RET_LIMITE;

  IF NOVO_SALDO IS NULL THEN RETURN; END IF;

  INSERT INTO TRANSACAO (CLIENTE_ID, VALOR, TIPO, DESCRICAO)
  VALUES(CLIENTE_ID, ABS(VALOR), TIPO, DESCRICAO);
END;
$$;

-- DATABASE
ALTER DATABASE RINHA SET SYNCHRONOUS_COMMIT=OFF;
