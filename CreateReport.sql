
-- 1. Совершение операции при просроченном или заблокированном паспорте.
INSERT INTO DEMIPT.PVVL_REP_FRAUD (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
SELECT DISTINCT
--    to_char(trans_date, 'YYYY.MM.DD HH24:MI:SS' ),
    tr.trans_date event_dt ,
    cl.passport_num passport,
    cl.last_name || ' ' ||  cl.first_name || ' ' || cl.patronymic fio,
    cl.phone phone,
    '1' event_type,
    sysdate report_dt
FROM DEMIPT.PVVL_DWH_FCT_TRANSACTIONS tr
INNER JOIN DEMIPT.PVVL_DWH_DIM_CARDS_HIST cd
    ON tr.card_num = trim(cd.card_num) AND tr.trans_date between cd.effective_from AND cd.effective_to
INNER JOIN PVVL_DWH_DIM_ACCOUNTS_HIST ac 
    ON cd.account = ac.account AND tr.trans_date between ac.effective_from AND ac.effective_to
INNER JOIN PVVL_DWH_DIM_CLIENTS_HIST cl
    ON ac.client = cl.client_id AND tr.trans_date between cl.effective_from AND cl.effective_to
WHERE tr.trans_date > cl.passport_valid_to
OR cl.PASSPORT_NUM in (select PASSPORT_NUM from DEMIPT.PVVL_DWH_FCT_PSSPRT_BLACKLIST);


--2. Совершение операции при недействующем договоре.
INSERT INTO DEMIPT.PVVL_REP_FRAUD (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
SELECT 
    tr.trans_date event_dt ,
    cl.passport_num passport,
    cl.last_name || ' ' ||  cl.first_name || ' ' || cl.patronymic fio,
    cl.phone phone,
    '2' event_type,
    sysdate report_dt
FROM DEMIPT.PVVL_DWH_FCT_TRANSACTIONS tr
INNER JOIN DEMIPT.PVVL_DWH_DIM_CARDS_HIST cd
    ON tr.card_num = trim(cd.card_num) AND tr.trans_date between cd.effective_from AND cd.effective_to
INNER JOIN PVVL_DWH_DIM_ACCOUNTS_HIST ac 
    ON cd.account = ac.account AND tr.trans_date between ac.effective_from AND ac.effective_to
INNER JOIN PVVL_DWH_DIM_CLIENTS_HIST cl
    ON ac.client = cl.client_id AND tr.trans_date between cl.effective_from AND cl.effective_to
WHERE tr.trans_date > ac.VALID_TO;


--3. Совершение операций в разных городах в течение одного часа.
INSERT INTO DEMIPT.PVVL_REP_FRAUD (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
SELECT 
    tr.trans_date event_dt ,
    cl.passport_num passport,
    cl.last_name || ' ' ||  cl.first_name || ' ' || cl.patronymic fio,
    cl.phone phone,
    '3' event_type,
    sysdate report_dt
FROM DEMIPT.PVVL_DWH_FCT_TRANSACTIONS tr
INNER JOIN DEMIPT.PVVL_DWH_DIM_CARDS_HIST cd
    ON tr.card_num = trim(cd.card_num) AND tr.trans_date between cd.effective_from AND cd.effective_to
INNER JOIN PVVL_DWH_DIM_ACCOUNTS_HIST ac 
    ON cd.account = ac.account AND tr.trans_date between ac.effective_from AND ac.effective_to
INNER JOIN PVVL_DWH_DIM_CLIENTS_HIST cl
    ON ac.client = cl.client_id AND tr.trans_date between cl.effective_from AND cl.effective_to
WHERE tr.trans_id in (
    SELECT tr1.trans_id 
    FROM DEMIPT.PVVL_DWH_FCT_TRANSACTIONS  tr1 
    INNER JOIN pvvl_dwh_dim_terminals_hist te1
        ON te1.terminal_id = tr1.terminal AND tr1.trans_date between te1.effective_from AND te1.effective_to
    INNER JOIN DEMIPT.PVVL_DWH_FCT_TRANSACTIONS  tr2
        ON tr1.card_num = tr2.card_num AND ABS(tr1.trans_date - tr2.trans_date)*24 < 1
     INNER JOIN pvvl_dwh_dim_terminals_hist te2
        ON te2.terminal_id = tr2.terminal AND tr2.trans_date between te2.effective_from AND te2.effective_to
    WHERE te1.terminal_city <> te2.terminal_city
);





--4. Попытка подбора суммы. В течение 20 минут проходит более 3х операций
INSERT INTO DEMIPT.PVVL_REP_FRAUD (EVENT_DT, PASSPORT, FIO, PHONE, EVENT_TYPE, REPORT_DT)
SELECT 
    tr.trans_date event_dt ,
    cl.passport_num passport,
    cl.last_name || ' ' ||  cl.first_name || ' ' || cl.patronymic fio,
    cl.phone phone,
    '4' event_type,
    sysdate report_dt
FROM DEMIPT.PVVL_DWH_FCT_TRANSACTIONS tr
INNER JOIN DEMIPT.PVVL_DWH_DIM_CARDS_HIST cd
    ON tr.card_num = trim(cd.card_num) AND tr.trans_date between cd.effective_from AND cd.effective_to
INNER JOIN PVVL_DWH_DIM_ACCOUNTS_HIST ac 
    ON cd.account = ac.account AND tr.trans_date between ac.effective_from AND ac.effective_to
INNER JOIN PVVL_DWH_DIM_CLIENTS_HIST cl
    ON ac.client = cl.client_id AND tr.trans_date between cl.effective_from AND cl.effective_to
WHERE tr.trans_id in (
	SELECT t4 FROM    
        (
        SELECT 
            oper_result r1,
            lead(oper_result) over(PARTITION BY card_num ORDER BY trans_date) r2,
            lead(oper_result,2) over(PARTITION BY card_num ORDER BY trans_date) r3,
            lead(oper_result,3) over(PARTITION BY card_num ORDER BY trans_date) r4,
            lead(trans_id,3) over(PARTITION BY card_num ORDER BY trans_date) t4,
            trans_date d1,
            lead(trans_date,3) over(PARTITION BY card_num ORDER BY trans_date) d4
        FROM DEMIPT.PVVL_DWH_FCT_TRANSACTIONS
        ) t
	WHERE 1=1
	    AND R1 = 'REJECT'
	    AND R2 = 'REJECT'
	    AND R3 = 'REJECT'
	    AND R4 = 'SUCCESS'
	    AND ABS(D4-D1)*24*60 < 20

 );