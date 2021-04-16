
-- Выполяем инкрементальную загрузку SCD2
-- TODO удаления

-- начало транзакции

-- 1. Очистка данных из STG

delete from DEMIPT.PVVL_STG_CARDS;
delete from DEMIPT.PVVL_STG_ACCOUNTS;
delete from DEMIPT.PVVL_STG_CLIENTS;
delete from DEMIPT.PVVL_DEL_CARDS;
delete from DEMIPT.PVVL_DEL_ACCOUNTS;
delete from DEMIPT.PVVL_DEL_CLIENTS;



-- 2. Захват данных из источника в STG
insert into DEMIPT.PVVL_STG_CARDS( CARD_NUM, ACCOUNT, update_dt, create_dt)
select CARD_NUM, ACCOUNT, update_dt, create_dt
from BANK.CARDS
where coalesce( update_dt, create_dt ) > (
	select last_update from DEMIPT.PVVL_META_LASTCDC where dbname = 'DEMIPT' and table_name = 'PVVL_STG_CARDS'
) OR (select last_update from DEMIPT.PVVL_META_LASTCDC where dbname = 'DEMIPT' and table_name = 'PVVL_STG_CARDS')
	 =  to_date('01.01.1900', 'DD.MM.YYYY');

insert into DEMIPT.PVVL_STG_ACCOUNTS(ACCOUNT, VALID_TO, CLIENT, UPDATE_DT, create_dt)
select ACCOUNT, VALID_TO, CLIENT, UPDATE_DT, create_dt
from BANK.ACCOUNTS
where coalesce( update_dt, create_dt ) > (
	select last_update from DEMIPT.PVVL_META_LASTCDC where dbname = 'DEMIPT' and table_name = 'PVVL_STG_ACCOUNTS'
) OR (select last_update from DEMIPT.PVVL_META_LASTCDC where dbname = 'DEMIPT' and table_name = 'PVVL_STG_ACCOUNTS')
	 =  to_date('01.01.1900', 'DD.MM.YYYY');

insert into DEMIPT.PVVL_STG_CLIENTS(CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, 
	PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, UPDATE_DT, create_dt)
select CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, UPDATE_DT, create_dt
from BANK.CLIENTS
where coalesce( update_dt, create_dt ) > (
	select last_update from DEMIPT.PVVL_META_LASTCDC where dbname = 'DEMIPT' and table_name = 'PVVL_STG_CLIENTS'
) OR (select last_update from DEMIPT.PVVL_META_LASTCDC where dbname = 'DEMIPT' and table_name = 'PVVL_STG_CLIENTS')
	 =  to_date('01.01.1900', 'DD.MM.YYYY');





-- 3. Вливаем данные в хранилище

insert into DEMIPT.PVVL_DWH_DIM_CARDS_HIST(CARD_NUM, ACCOUNT, effective_from, effective_to, deleted_flg )
select
	CARD_NUM, ACCOUNT, coalesce( update_dt, create_dt ),
	to_date( '2999-12-31', 'YYYY-MM-DD' ),
	'N'
from DEMIPT.PVVL_STG_CARDS;
merge into DEMIPT.PVVL_DWH_DIM_CARDS_HIST t1
using DEMIPT.PVVL_STG_CARDS t2
on ( t1.CARD_NUM = t2.CARD_NUM and t1.effective_from < coalesce(t2.update_dt, t2.create_dt ) )
when matched then update set 
    t1.effective_to = t2.update_dt - interval '1' second
	where t1.effective_to = to_date( '2999-12-31', 'YYYY-MM-DD' );


insert into DEMIPT.PVVL_DWH_DIM_ACCOUNTS_HIST(ACCOUNT, VALID_TO, CLIENT, effective_from, effective_to, deleted_flg )
select
	ACCOUNT, VALID_TO, CLIENT, coalesce( update_dt, create_dt ),
	to_date( '2999-12-31', 'YYYY-MM-DD' ),
	'N'
from DEMIPT.PVVL_STG_ACCOUNTS;
merge into DEMIPT.PVVL_DWH_DIM_ACCOUNTS_HIST t1
using DEMIPT.PVVL_STG_ACCOUNTS t2
on ( t1.ACCOUNT = t2.ACCOUNT and t1.effective_from < coalesce(t2.update_dt, t2.create_dt ) )
when matched then update set 
    t1.effective_to = t2.update_dt - interval '1' second
	where t1.effective_to = to_date( '2999-12-31', 'YYYY-MM-DD' );



insert into DEMIPT.PVVL_DWH_DIM_CLIENTS_HIST (CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, effective_from, effective_to, deleted_flg )
select
	CLIENT_ID, LAST_NAME, FIRST_NAME, PATRONYMIC, DATE_OF_BIRTH, PASSPORT_NUM, PASSPORT_VALID_TO, PHONE, coalesce( update_dt, create_dt ),
	to_date( '2999-12-31', 'YYYY-MM-DD' ),
	'N'
from DEMIPT.PVVL_STG_CLIENTS;
merge into DEMIPT.PVVL_DWH_DIM_CLIENTS_HIST t1
using DEMIPT.PVVL_STG_CLIENTS t2
on ( t1.CLIENT_ID = t2.CLIENT_ID and t1.effective_from < coalesce(t2.update_dt, t2.create_dt ) )
when matched then update set 
    t1.effective_to = t2.update_dt - interval '1' second
	where t1.effective_to = to_date( '2999-12-31', 'YYYY-MM-DD' );





-- 4. Захватываем ключи для проверки удалений (опционально)
-- insert into stg1_del( id )
-- select id from source1

-- 5. Удаляем удаленные записи в целевой таблице (опционально)

-- ???????

-- 6. Обновляем метаданные - дату максимальной загрузуки
update DEMIPT.PVVL_META_LASTCDC 
set last_update = coalesce(( select max( coalesce( update_dt, create_dt ) ) from DEMIPT.PVVL_STG_CARDS ), to_date( '1900-01-01', 'YYYY-MM-DD' ))
where  dbname = 'DEMIPT' and table_name = 'PVVL_STG_CARDS';

update DEMIPT.PVVL_META_LASTCDC 
set last_update = coalesce(( select max( coalesce( update_dt, create_dt ) ) from DEMIPT.PVVL_STG_ACCOUNTS ), to_date( '1900-01-01', 'YYYY-MM-DD' ))
where  dbname = 'DEMIPT' and table_name = 'PVVL_STG_ACCOUNTS';

update DEMIPT.PVVL_META_LASTCDC 
set last_update = coalesce(( select max( coalesce( update_dt, create_dt ) ) from DEMIPT.PVVL_STG_CLIENTS ), to_date( '1900-01-01', 'YYYY-MM-DD' ))
where  dbname = 'DEMIPT' and table_name = 'PVVL_STG_CLIENTS';

-- 7. Фиксируется транзакция
commit;

