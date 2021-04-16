--------------PROCEED PASPORTS--------------



-- insert into dwh
merge into DEMIPT.PVVL_DWH_FCT_PSSPRT_BLACKLIST t1 using DEMIPT.PVVL_STG_PASSPORT_BLACKLIST t2 on (t1.PASSPORT_NUM = t2.PASSPORT_NUM)
when matched then update set 					
t1.UPDATE_DT = t2.UPDATE_DT
when not matched then insert (PASSPORT_NUM, ENTRY_DT, create_dt)
values(t2.PASSPORT_NUM, t2.ENTRY_DT,t2.UPDATE_DT);

-- ------------Запись в мета passports--------------
update DEMIPT.PVVL_META_LASTCDC 
set last_update = (select max(update_dt) from DEMIPT.PVVL_STG_PASSPORT_BLACKLIST)
where  dbname = 'DEMIPT' and table_name = 'PVVL_STG_PASSPORT_BLACKLIST';

commit;
