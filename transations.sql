--------------PROCEED TRANSACTIONS--------------
--load to DWH
merge into DEMIPT.PVVL_DWH_FCT_TRANSACTIONS t1 using DEMIPT.PVVL_STG_TRANSACTIONS t2 on (t1.trans_id = t2.trans_id)
when matched then update set 					
		    t1.trans_date = t2.trans_date,
		    t1.card_num = t2.card_num,
		    t1.oper_type = t2.oper_type,
		    t1.amt = t2.amt,
		    t1.oper_result = t2.oper_result,
		    t1.terminal = t2.terminal,
		    t1.update_dt = t2.update_dt
when not matched then insert (trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal, create_dt)
values(t2.trans_id, t2.trans_date, t2.card_num, t2.oper_type, t2.amt, t2.oper_result, t2.terminal, t2.update_dt);

-- ------------writwe to meta--------------
update DEMIPT.PVVL_META_LASTCDC 
set last_update = (select max(update_dt) from DEMIPT.PVVL_STG_TRANSACTIONS )
where  dbname = 'DEMIPT' and table_name = 'PVVL_STG_TRANSACTIONS';


commit;
