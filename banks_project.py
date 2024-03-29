from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

# Initialise variables

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks_data'
csv_path = './Largest_banks_data.csv'
sql_connection = sqlite3.connect('Banks.db')


# Code for ETL operations on Country-GDP data

# Importing the required libraries

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')


def extract(url, table_attribs):
    # Extracts the required information from the website and saves it to a dataframe

    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')
    for row in rows[1:]:
        col = row.find_all('td')
        data_dict = {"Name": col[1].text,
                    "MC_USD_Billion": float(col[2].text)}
        df1 = pd.DataFrame(data_dict, index=[0])
        df = pd.concat([df,df1], ignore_index=True)
        df['Name'] = df['Name'].str.replace('\n', '')
    return df


def transform(df):

    exchange_rate_df = pd.read_csv(r'C:\Users\Javi\Documents\GitHub\Data-engineering-5-WorldsLargestBanks\exchange_rate.csv')
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]

    return df

def load_to_csv(df, csv_path): # transform the df into a csv to be able to load it into the database
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name): # load csv into the database
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection): # function to read the Queries
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)




log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df = transform(df)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as table. Executing queries')

query_statement = f"SELECT * from {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()

log_progress('Server Connection closed')

