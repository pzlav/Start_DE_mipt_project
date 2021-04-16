#!/usr/bin/env python3
import sys
import os
import datetime
import subprocess
import re
import pandas as pd
import jaydebeapi
from string import printable

print('\n'*20)
script_dir = os.path.dirname(__file__)

def to_log(stext: str):
	f = open("log.txt", "a", encoding='utf8')
	res_str = str(datetime.datetime.now()) + " : " + stext + "\n"
	print(res_str)
	f.write(res_str)
	f.close()
	return

def exec_sql_file(filename: str, cursor):
	f = open(os.path.join(script_dir, "sql_scripts/"+filename), "r", encoding='utf8')
	s_text =  f.read()
	f.close()
	for sql in s_text.split(";"):
		nsql = ''.join(char for char in sql if char in printable)
		if nsql.replace('\n','') != '':
			to_log("\nExecute:\n"+nsql)
			curs.execute(nsql)
	return

to_log('----------------------------------------------')
to_log('НАЧАЛО РАБОТЫ')
to_log('----------------------------------------------')
to_log('Копирование входных файлов в архив')
#process = subprocess.call(os.path.join(script_dir, "py_scripts/copy.sh"))
#Альтернативный вариант с записью в лог
temp_file = open("log.txt",'a')
process = subprocess.call(os.path.join(script_dir, "py_scripts/copy.sh"), stdout=temp_file)
temp_file.close()

files = [f for f in os.listdir('.') if re.search(r'[0-3][0-9]', f) is not None]
to_log('Обработка файлов: ' + str(files))

li_transactions = []
li_passport_blacklist = []
li_terminals = []
for f in files:
	db_name = f.split("_")[0]
	f_date = datetime.datetime.strptime(f.split("_")[-1].split(".")[0], "%d%m%Y")
	f_ext = f.split(".")[-1]
	print("Текущие параметры: ", f, db_name, f_date, f_ext, '\n')
	if db_name == 'transactions': # ТУТ ДАННЫЕ ЗА 1 ДЕНЬ!
		df = pd.read_csv(f, index_col=None, header=1, sep=';')
		df.columns = ['transaction_id','transaction_date','amount','card_num','oper_type','oper_result','terminal']
		df.loc[:,'file_date'] = f_date
		li_transactions.append(df)
	if db_name == 'passport':  #TУТ ВСЯ ИСТОРИЯ НЕТ СМЫСЛА ОБЪЕДИНЯТЬ, НО МОЖНО ПОИСКАТЬ УДАЛЕННЫЕ
		df = pd.read_excel(f, sheet_name='blacklist', header=1, index_col=None)
		df.columns = ['date','passport']
		df.loc[:,'file_date'] = f_date
		li_passport_blacklist.append(df)
	if db_name == 'terminals':  #ТУТ ДАННЫЕ С НАЧАЛА МЕСЯЦА #ВИДИМО ТУТ МОЖНО ПОИСКАТЬ УДАЛЕННЫЕ КАК-ТО
		df = pd.read_excel(f, sheet_name='terminals', header=1, index_col=None) 
		df.columns = ['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address']
		df.loc[:,'file_date'] = f_date
		li_terminals.append(df)

df_transactions = pd.concat(li_transactions, axis=0, ignore_index=True)
df_transactions = df_transactions.sort_values(by='file_date')
df_transactions['amount'] = (df_transactions['amount'].str.split()).apply(lambda x: float(x[0].replace(',', '.')))
df_transactions['file_date'] = df_transactions['file_date'].apply(str)

df_passport_blacklist = pd.concat(li_passport_blacklist, axis=0, ignore_index=True)
df_passport_blacklist = df_passport_blacklist.sort_values(by='file_date')
df_passport_blacklist['file_date'] = df_passport_blacklist['file_date'].apply(str)
df_passport_blacklist['date'] = df_passport_blacklist['date'].apply(str)

df_terminals = pd.concat(li_terminals, axis=0, ignore_index=True)
df_terminals = df_terminals.sort_values(by='file_date')
df_terminals['file_date'] = df_terminals['file_date'].apply(str)


to_log('--------------Подключение к базе--------------')
conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver','jdbc:oracle:thin:demipt/gandalfthegrey@de-oracle.chronosavant.ru:1521/deoracle',['demipt','gandalfthegrey'],'ojdbc8.jar')
conn.jconn.setAutoCommit(False)
curs = conn.cursor()


to_log('------------------------------------------------------------')
to_log('------------1 Обработка базы Transactions SCD1--------------')
for d in df_transactions['file_date'].unique():
	to_log("Обработка за дату: {}".format(d))
	cdf = df_transactions.loc[df_transactions['file_date'] == d]
	to_log('--------------Очистка стейнджинга Transactions--------------')
	curs.execute("""delete from DEMIPT.PVVL_STG_TRANSACTIONS""")
	curs.execute("""delete from DEMIPT.PVVL_DEL_TRANSACTIONS""")
	curs.execute("""commit""")
	curs.execute("""SELECT last_update FROM DEMIPT.PVVL_META_LASTCDC where  dbname = 'DEMIPT' and table_name = 'PVVL_STG_TRANSACTIONS'""")
	result = curs.fetchall()
	c_date = str(result[0][0])
	if datetime.datetime.strptime(d,"""%Y-%m-%d %M:%S:%H""") > datetime.datetime.strptime(c_date,"""%Y-%m-%d %M:%S:%H"""): 	#проверка что апдейтим только то чего ещё не было
		dlist = cdf[['transaction_id','transaction_date', 'card_num','oper_type', 'amount','oper_result','terminal','file_date']].values.tolist()
		to_log('--------------Загрузка данных в стейнджинг Transactions--------------')
		curs.executemany("""insert into DEMIPT.PVVL_STG_TRANSACTIONS( 
				trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal, update_dt) 
				values(?, to_date(?, 'YYYY-MM-DD HH24:MI:SS'),?,?,?,?,?,to_date(?, 'YYYY-MM-DD HH24:MI:SS'))""", dlist)
		curs.execute("""commit""")
		exec_sql_file('transations.sql',curs)
	else:
		to_log('CRITICAL ERROR: повторный ввод данных')
		to_log('Дата в файле: {}'.format(d))
		to_log('Дата в файле: {}'.format(c_date))

to_log('----------------------------------------------------------------')
to_log('-----------2 Обработка базы Passport_blacklist SCD1-------------')
for d in df_passport_blacklist['file_date'].unique():
	to_log("Обработка за дату: {}".format(d))
	cdf = df_passport_blacklist.loc[df_passport_blacklist['file_date'] == d]
	to_log('--------------Очистка стейнджинга Passport_blacklist--------------')
	curs.execute("""delete from DEMIPT.PVVL_STG_PASSPORT_BLACKLIST""")
	curs.execute("""commit""")
	dlist = cdf[['passport','date','file_date']].values.tolist()
	to_log('--------------Загрузка данных в стейнджинг Passport_blacklist--------------')
	curs.executemany("""insert into demipt.PVVL_STG_PASSPORT_BLACKLIST(PASSPORT_NUM, ENTRY_DT, UPDATE_DT) 
			values(?,to_date(?, 'YYYY-MM-DD HH24:MI:SS'), to_date(?, 'YYYY-MM-DD HH24:MI:SS'))""", dlist)
	curs.execute("""commit""")
	exec_sql_file('passports.sql',curs)

to_log('------------------------------------------------------------')
to_log('-----------3 Обработка базы Terminals SCD2--------------')
for d in df_terminals['file_date'].unique():
	to_log("Обработка за дату: {}".format(d))
	cdf = df_terminals.loc[df_terminals['file_date'] == d]
	to_log('--------------Очистка стейнджинга Terminals--------------')
	curs.execute("""delete from DEMIPT.PVVL_STG_TERMINALS""")
	curs.execute("""delete from DEMIPT.PVVL_DEL_TERMINALS""")
	curs.execute("""commit""")
	#TODO ПРОВЕРКА НА NA
	curs.execute("""SELECT last_update FROM DEMIPT.PVVL_META_LASTCDC where  dbname = 'DEMIPT' and table_name = 'PVVL_STG_TERMINALS'""")
	result = curs.fetchall()
	c_date = str(result[0][0])
	if datetime.datetime.strptime(d,"""%Y-%m-%d %M:%S:%H""") > datetime.datetime.strptime(c_date,"""%Y-%m-%d %M:%S:%H"""): 	#проверка что апдейтим только то чего ещё не было
		dlist = cdf[['terminal_id', 'terminal_type', 'terminal_city', 'terminal_address', 'file_date']].values.tolist()
		to_log('--------------Загрузка данных в стейнджинг Terminals--------------')
		curs.executemany("""insert into demipt.PVVL_STG_TERMINALS(terminal_id, terminal_type, terminal_city, terminal_address,update_dt) 
			values(?,?,?,?,to_date(?, 'YYYY-MM-DD HH24:MI:SS'))""", dlist)
		curs.execute("""commit""")
		exec_sql_file('terminals.sql',curs) #ЗДЕСЬ ЧЕСТНЫЙ SCD2
	else:
		to_log('CRITICAL ERROR: повторный ввод данных')
		to_log('Дата в файле: {}'.format(d))
		to_log('Дата в файле: {}'.format(c_date))
	

to_log('-------------4 Загрузка данных из cхемы BANK--------------')
exec_sql_file('LoadFromBANK.sql',curs)


to_log('-------------5 Построение отчета о мошшениках--------------')
exec_sql_file('CreateReport.sql',curs)



to_log('--------------Закрытие соединения--------------')
curs.close()
conn.close()




#sys.path.insert(0, './py_scripts')
#import drop_all_tables



