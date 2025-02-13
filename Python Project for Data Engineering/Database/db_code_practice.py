import sqlite3
import pandas as pd 

conn = sqlite3.connect('STAFF.db')

table_name = 'DEPARTMENT'
attributes = ['DEPT_ID', 'DEP_NAME', 'MANAGER_ID', 'LOC_ID']

file_path = '/home/project/Departments.csv'
df = pd.read_csv(file_path, names=attributes)

df.to_sql(table_name, conn, if_exists='replace', index=False)
print('Table is ready')

data_dict = {'DEPT_ID': [9],
'DEP_NAME': ['QUALITY ASSURANCE'],
'MANAGER_ID': [30010],
'LOC_ID': ['L0010']}
data_append = pd.DataFrame(data_dict)

data_append.to_sql(table_name, conn, if_exists='append', index=False)
print("Data Appended")

query1 = f"SELECT * FROM {table_name}"
query2 = f"SELECT DEP_NAME FROM {table_name}"
query3 = f"SELECT COUNT(*) FROM {table_name}"

print("ALL DETAILS from DEPARTMENT \n",pd.read_sql(query1, conn))
print("Department Names \n", pd.read_sql(query2,conn))
print("Count of records \n", pd.read_sql(query3,conn))