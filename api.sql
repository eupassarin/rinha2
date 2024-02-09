SELECT
    T.id,
    T.cliente_id,
    T.valor,
    T.tipo,
    T.realizada_em,
    T.descricao
FROM transacao T
WHERE T.cliente_id = 1
ORDER BY T.id DESC
LIMIT 10;

SELECT
    C.id,
    C.saldo,
    C.limite
FROM cliente C
WHERE C.id = 1;

--SELECT pg_advisory_lock(C.id), C.saldo, C.limite FROM cliente C WHERE C.id = 1;

begin;
rollback;
--SELECT pg_advisory_lock(1);
-- pg_advisory_unlock(1);

select * from pg_locks join pg_stat_activity using (pid) where locktype='advisory';
SELECT pg_terminate_backend(559);


DROP TABLE TRANSACAO;
DROP TABLE CLIENTE;





