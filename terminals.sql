--------------------------------------------------
--------------Proceed terminals.sql--------------
--------------Load from STG to DWH--------------

-- insert new id
insert into 
	DEMIPT.PVVL_DWH_DIM_TERMINALS_HIST(terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
select	
	t2.terminal_id,
	t2.terminal_type,
	t2.terminal_city,
	t2.terminal_address,
	t2.update_dt,
	to_date( '2999-12-31', 'YYYY-MM-DD' ),
	'N'
from DEMIPT.PVVL_STG_TERMINALS t2 left join DEMIPT.PVVL_DWH_DIM_TERMINALS_HIST t1
on t1.terminal_id = t2.terminal_id
where  t1.terminal_id is null;


-- capture id that had been updated
insert into 
	DEMIPT.PVVL_DEL_TERMINALS(terminal_id)
select	t2.terminal_id
from DEMIPT.PVVL_STG_TERMINALS t2 left join DEMIPT.PVVL_DWH_DIM_TERMINALS_HIST t1
on t1.terminal_id = t2.terminal_id
where  1=1 
	AND t1.terminal_type <> t2.terminal_type
	AND t1.terminal_city <> t2.terminal_city
	AND t1.terminal_address <>  t2.terminal_address;

-- close rows that had been updated
update DEMIPT.PVVL_DWH_DIM_TERMINALS_HIST t1
set
    t1.terminal_type = (select terminal_type from DEMIPT.PVVL_STG_TERMINALS t2 where t1.terminal_id = t2.terminal_id),
    t1.terminal_city = (select terminal_city from DEMIPT.PVVL_STG_TERMINALS  t2 where t1.terminal_id = t2.terminal_id),
    t1.terminal_address = (select terminal_address from DEMIPT.PVVL_STG_TERMINALS t2 where t1.terminal_id = t2.terminal_id),
    t1.effective_to = (select update_dt from DEMIPT.PVVL_STG_TERMINALS t2 where t1.terminal_id = t2.terminal_id) - interval '1' second
 where 1=1
        AND t1.effective_to = to_date( '2999-12-31', 'YYYY-MM-DD')
        AND t1.terminal_id IN (select terminal_id FROM DEMIPT.PVVL_DEL_TERMINALS);
 				
-- insert updated id
insert into 
	DEMIPT.PVVL_DWH_DIM_TERMINALS_HIST(terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
select	
	t2.terminal_id,
	t2.terminal_type,
	t2.terminal_city,
	t2.terminal_address,
	t2.update_dt,
	to_date( '2999-12-31', 'YYYY-MM-DD' ),
	'N'
from DEMIPT.PVVL_STG_TERMINALS t2 left join DEMIPT.PVVL_DWH_DIM_TERMINALS_HIST t1
on t1.terminal_id = t2.terminal_id
where  t2.terminal_id IN (select terminal_id FROM DEMIPT.PVVL_DEL_TERMINALS);


--------------Write meta of Terminals--------------
update PVVL_META_LASTCDC 
 	set last_update = (select max(update_dt) from DEMIPT.PVVL_STG_TERMINALS)
 	where  dbname = 'DEMIPT' and table_name = 'PVVL_STG_TERMINALS';


-------------- Deleted rows--------------	
delete from DEMIPT.PVVL_DEL_TERMINALS;
-- capture only deleted id
insert into DEMIPT.PVVL_DEL_TERMINALS(terminal_id)
select terminal_id from DEMIPT.PVVL_DWH_DIM_TERMINALS_HIST
where terminal_id in (select t1.terminal_id 
                from DEMIPT.PVVL_STG_TERMINALS t2 right join DEMIPT.PVVL_DWH_DIM_TERMINALS_HIST t1
                on t1.terminal_id = t2.terminal_id
                where t2.terminal_id is null);

insert into 
	DEMIPT.PVVL_DWH_DIM_TERMINALS_HIST(terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg )
select	
	t1.terminal_id,
	t1.terminal_type,
	t1.terminal_city,
	t1.terminal_address,
	sysdate,
	to_date( '2999-12-31', 'YYYY-MM-DD' ),
	'Y'
from DEMIPT.PVVL_DWH_DIM_TERMINALS_HIST t1
where  t1.terminal_id IN (select terminal_id FROM DEMIPT.PVVL_DEL_TERMINALS)
AND t1.effective_to = to_date( '2999-12-31', 'YYYY-MM-DD');

update DEMIPT.PVVL_DWH_DIM_TERMINALS_HIST t1 
set 			
    --t1.deleted_flg = 'Y',
    t1.effective_to = sysdate - interval '1' second
where  t1.terminal_id IN (select terminal_id FROM DEMIPT.PVVL_DEL_TERMINALS)
AND t1.deleted_flg = 'N';




commit;