import psycopg2

conn = psycopg2.connect(database="dwh",
                        user="sfakhrutdinov", password="pass",
                        host="localhost", port="5433"
                        )

conn.autocommit = True
cursor = conn.cursor()

sql_delete_table = '''DROP TABLE IF EXISTS rd.deal_info_new;'''

cursor.execute(sql_delete_table)

sql_create_table = '''CREATE TABLE rd.deal_info_new(deal_rk bigint NOT NULL,\
deal_num text,\
deal_name text,\
deal_sum numeric,\
client_rk bigint NOT NULL,\
account_rk bigint NOT NULL,\
agreement_rk bigint NOT NULL,\
deal_start_date date,\
department_rk bigint,\
product_rk bigint,\
deal_type_cd text,\
effective_from_date date NOT NULL,\
effective_to_date date NOT NULL);'''

cursor.execute(sql_create_table)

sql_insert_data = '''COPY rd.deal_info_new(deal_rk,\
deal_num,\
deal_name,\
deal_sum,\
client_rk,\
account_rk,\
agreement_rk,\
deal_start_date,\
department_rk,\
product_rk,\
deal_type_cd,\
effective_from_date,\
effective_to_date) 
FROM '/Users/sfakhrutdinov/Downloads/project_2/data/new_data_loan_holiday_info/deal_info_new.csv' 
DELIMITER ',' 
CSV HEADER
ENCODING 'windows-1251';'''

cursor.execute(sql_insert_data)

sql_select_data = '''select * from rd.deal_info_new;'''
cursor.execute(sql_select_data)
for i in cursor.fetchall():
    print(i)

conn.commit()
conn.close()
