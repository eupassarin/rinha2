-- SELECT
--     T.id,
--     T.cliente_id,
--     T.valor,
--     T.tipo,
--     T.realizada_em,
--     T.descricao
-- FROM transacao T
-- WHERE T.cliente_id = 1
-- ORDER BY T.id DESC
-- LIMIT 10;
--
-- SELECT
--     C.id,
--     C.saldo,
--     C.limite
-- FROM cliente C
-- WHERE C.id = 1;
--
-- --SELECT pg_advisory_lock(C.id), C.saldo, C.limite FROM cliente C WHERE C.id = 1;
--
-- begin;
-- rollback;
-- --SELECT pg_advisory_lock(1);
-- -- pg_advisory_unlock(1);
--
-- select * from pg_locks join pg_stat_activity using (pid) where locktype='advisory';
-- SELECT pg_terminate_backend(559);
--
-- CALL TRANSACIONAR(1, -1000000000, 'D', 'Compra');
--
--
-- DROP TABLE TRANSACAO;
-- DROP TABLE CLIENTE;

truncate cliente CASCADE;
INSERT INTO CLIENTE (ID, LIMITE) VALUES(1, 1000*100),(2, 800*100),(3, 10000*100),(4, 100000*100),(5, 5000*100);






