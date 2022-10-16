

import requests
import json
from requests.exceptions import HTTPError
import sqlite3 
from sys import argv
from os import getenv
from dotenv import load_dotenv 
from create1 import Create
from wskaznik import Wskaznik


load_dotenv()
if len(argv) == 2 and argv[1] == 'setup':
    """
    Initialize db
    py req1.py setup
    """
    print('Tworze tabele w bazie danych')
    Create.createTable()


if len(argv) == 2 and argv[1] == 'insert':
    """
    downloading and inserting data into db
    py req1.py insert
    """
    topCount = (int(input('Z ilu ostatnich dni dane mają być zaciągnięte?\n')))
   
    def get_rates_of_gold(topCount):
        try:
        
            url = f"http://api.nbp.pl/api/cenyzlota/last/{topCount}"
            response = requests.get(url)
        except HTTPError as http_error:
            print(f'HTTP error: {http_error}')
        except Exception as e:
            print(f'Other exception: {e}')
        else:
            if response.status_code == 200:
                return json.dumps(
                    response.json(),
                    indent=4,
                    sort_keys=True), response.json()
    
    def get_rates_of_currency(topCount):
    
        try:
            table = "A"
            currency = 'USD'
            rates_number = topCount # Top.topCount do wymiany na input na ten moment
            url = f"http://api.nbp.pl/api/exchangerates/rates/{table}/{currency}/last/{rates_number}/"
            response = requests.get(url)
        except HTTPError as http_error:
            print(f'HTTP error: {http_error}')
        except Exception as e:
            print(f'Other exception: {e}')
        else:
            if response.status_code == 200:
                return json.dumps(
                    response.json(),
                    indent=4,
                    sort_keys=True), response.json()
 
    def insertVaribleIntoTable(value, date, usd, mid, effectivedate):
        try:
        # Connecting to sqlite
            conn = sqlite3.connect('db1.db')
        # Creating a cursor object using the cursor() method
            cursor = conn.cursor()
            print("Connected to db1")

            sqlite_insert_with_param = """INSERT INTO GOLD
                            (VALUE, DATE, USD, MID, EFFECTIVEDATE) 
                            VALUES (?, ?, ?, ?, ?);"""

            data_tuple = (value, date, usd, mid, effectivedate)
            cursor.execute(sqlite_insert_with_param, data_tuple)
            
            conn.commit()
            print("Python Variables inserted successfully into SqliteDb_developers table")
            Wskaznik.select_all_tasks(conn)
            cursor.close()

        except sqlite3.Error as error:
            print("Failed to insert Python variable into sqlite table", error)
        finally:
            if conn:
                conn.close()
                print("The SQLite connection is closed")
    
        

    if __name__ == '__main__':
    # JSON jako string oraz JSON
        json_caly, Gold = get_rates_of_gold(topCount)#=(int(input('Podaj ilość tych walonych dni, proszę\n'))))# get_rates_of_gold(int(input('Z ilu dni?'))) #
        #print(json_caly)
        # Kurs Złota z <topCount> dni.
        print('a teraz currency')
            
        json_caly1, Dol = get_rates_of_currency(topCount)
        #print(json_caly1)
    # Kursy USD z <rates_number> dni.
        for i in range(len(Gold)):
            insertVaribleIntoTable(Gold[i]['cena'],Gold[i]['data'],'USD',Dol['rates'][i]['mid'], Dol['rates'][i]['effectiveDate'])
            


    

