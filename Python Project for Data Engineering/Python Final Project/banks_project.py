import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import os

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'Banks.db'
csv_name = 'Largest_banks_data.csv'
table_name = 'Largest_Banks'
attribute_list_extract = ['Country', 'MC_USD_Billion']
attribute_list_final = ['Country','MC_USD_Billion','MC_GBP_Billion','MC_EUR_Billion','MC_INR_Billion']
csv_path = './Largest_banks_data.csv'
log_file = "code_log.txt"


def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    df = pd.DataFrame(columns=attribute_list_extract)
    count = 0

    for row in rows:
        if count < 10:
            col = row.find_all('td')
            if len(col) != 0:
                data_dict = {"Country": col[1].find_all('a')[1].get('title'),
                             "MC_USD_Billion": float(col[2].contents[0])}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
                count += 1
        else:
            break
    return df

def transform(df, new_columns):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    columns_to_add = [col for col in new_columns if col not in attribute_list_extract]
    
    for column in columns_to_add:
        df[column] = None
    
    # Read exchange rates
    exchange_rates = pd.read_csv('exchange_rate.csv')
    exchange_rates.set_index('Currency', inplace=True)

    # Convert USD to other currencies
    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * exchange_rates.loc['GBP', 'Rate'],2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * exchange_rates.loc['EUR', 'Rate'],2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * exchange_rates.loc['INR', 'Rate'],2)

    return df

def load_to_csv(csv_path, df):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(csv_path, index=False)

def load_to_db(db_path, df, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def execute_query(db_path, query):
    ''' This function runs the query on the database table and
    prints the output on the terminal.'''
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    print(df.to_string(index=False))
    print("\n")
    return df

log_progress("ETL Job Started") 

log_progress('Extract phase Started')
df_extracted = extract(url)
log_progress('Extract phase Ended')

log_progress('Transform phase Started')
df_transformed = transform(df_extracted, attribute_list_final)
log_progress('Transform phase Ended')

log_progress("Load phase Started") 
load_to_csv(csv_path, df_transformed)
load_to_db(db_name, df_transformed, table_name)
log_progress("Load phase Ended")

log_progress("ETL Job Finished") 


log_progress("Query for the entire table Started")
query = "SELECT * FROM Largest_Banks"
execute_query(db_name, query)
log_progress("Query for the entire table Ended")

log_progress("Query for the average MC_GBP_Billion from all the banks Started")
query = "SELECT AVG(MC_GBP_Billion) FROM Largest_Banks"
execute_query(db_name, query)
log_progress("Query for the average MC_GBP_Billion from all the banks Ended")

log_progress("Query for the top 5 banks Started")
query = "SELECT * FROM Largest_Banks ORDER BY MC_USD_Billion DESC LIMIT 5"
execute_query(db_name, query)
log_progress("Query for the top 5 banks Ended")