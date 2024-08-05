import psycopg2

conn = psycopg2.connect(database="dwh",
                        user="sfakhrutdinov", password="pass",
                        host="localhost", port="5433"
                        )

conn.autocommit = True
cursor = conn.cursor()

sql_delete_table = '''DROP TABLE IF EXISTS rd.product_new;'''

cursor.execute(sql_delete_table)

sql_create_table = '''CREATE TABLE rd.product_new(product_rk bigint NOT NULL,\
product_name text,\
effective_from_date date NOT NULL,\
effective_to_date date NOT NULL);'''

cursor.execute(sql_create_table)

sql_insert_data = '''COPY rd.product_new(product_rk,\
product_name,\
effective_from_date,\
effective_to_date) 
FROM '/Users/sfakhrutdinov/Downloads/project_2/data/new_data_loan_holiday_info/product_info_new.csv' 
DELIMITER ',' 
CSV HEADER
ENCODING 'windows-1251';'''

cursor.execute(sql_insert_data)

sql_select_data = '''select * from rd.product_new;'''
cursor.execute(sql_select_data)
for i in cursor.fetchall():
    print(i)

conn.commit()
conn.close()
