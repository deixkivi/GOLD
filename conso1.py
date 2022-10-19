import sqlite3
from sqlite3 import Error
import requests
import json
from requests.exceptions import HTTPError
from sys import argv




# TWORZENIE TABELI
def createTable():
# Connecting to sqlite
    conn = sqlite3.connect('db1.db')
# Creating a cursor object using the cursor() method
    cursor = conn.cursor()
# Creating table
    table ="""CREATE TABLE GOLD(ID INTEGER PRIMARY KEY,
                                    VALUE INTEGER,
                                    DATE VARCHAR(255),
                                    CODE VARCHAR(255),
                                    MID INTEGER,
                                    EFFECTIVEDATE VARCHAR(255),
                                    WSKAZNIK INTEGER
                                    );"""
    cursor.execute(table)
    print('Tabela stworzona!')
# Commit your changes in the database    
    conn.commit()
# Closing the connection
    conn.close()

def insertVaribleIntoTable(value, date, code, mid, effectivedate):
        try:
        # Łączenie z bazą
            conn = sqlite3.connect('db1.db')
        # Tworzenie kursora
            cursor = conn.cursor()
            print("Połaczono z bazą danych db1")

            sqlite_insert_with_param = """INSERT INTO GOLD
                            (VALUE, DATE, CODE, MID, EFFECTIVEDATE) 
                            VALUES (?, ?, ?, ?, ?);"""
            #Tworzenie tupli
            data_tuple = (value, date, code, mid, effectivedate)
            # Wstawianie danych w tabelę
            cursor.execute(sqlite_insert_with_param, data_tuple)
            # Zapisywanie
            conn.commit()
            print("Dane skutecznie wprowadzone do bazy")
            cursor.close()

        except sqlite3.Error as error:
            print("Nie udało się wprowadzić danych do bazy", error)
        finally:
            if conn:
                conn.close()
                print("Zakończono połączenie z bazą")

#URUCHOMIENIE TWORZENIA TABELI KOMENDĄ: py conso1.py setup
if len(argv) == 2 and argv[1] == 'setup':
    """
    Initialize db
    py req1.py setup
    """
    print('Tworzę tabele w bazie danych')
    createTable()
#URUCHOMIENIE POBIERANIA I WSTAWIANIA DANYCH DO BAZY KOMENDĄ: py conso1.py insert
if len(argv) == 2 and argv[1] == 'insert':
    """
    downloading and inserting data into db
    py req1.py insert
    """
    topCount = (int(input('Z ilu ostatnich dni dane mają być zaciągnięte?\n')))    
    #Pobieranie informacji o cenie złota
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
    #Pobieranie informacji o cenie waluty USD
    def get_rates_of_usd(topCount):
        currency = 'USD'
        try:
            table = "A"                
            url = f"http://api.nbp.pl/api/exchangerates/rates/{table}/{currency}/last/{topCount}/"
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
    #Pobieranie informacji o cenie waluty GBP
    def get_rates_of_gbp(topCount):
        currency = 'GBP'
        try:
            table = "A"                
            url = f"http://api.nbp.pl/api/exchangerates/rates/{table}/{currency}/last/{topCount}/"
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
    #Pobieranie informacji o cenie waluty EUR
    def get_rates_of_eur(topCount):
        currency = 'EUR'
        try:
            table = "A"                
            url = f"http://api.nbp.pl/api/exchangerates/rates/{table}/{currency}/last/{topCount}/"
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
        
    if __name__ == '__main__':
# JSON jako string oraz JSON
        print('ZŁOTO')
        json_caly, Gold = get_rates_of_gold(topCount)
        # Kurs Złota z <topCount> dni.
        
        print('USD')    
        json_caly1, Dol = get_rates_of_usd(topCount)
        print('GBP')  
        json_caly2, Gbp = get_rates_of_gbp(topCount)
        print('EUR')  
        json_caly3, Eur = get_rates_of_eur(topCount)
        
    # Wstawienie kursów dla USD, GBP, EUR do tabeli
        for i in range(len(Gold)):
            insertVaribleIntoTable(Gold[i]['cena'],Gold[i]['data'],'USD',Dol['rates'][i]['mid'], Dol['rates'][i]['effectiveDate'])
        for i in range(len(Gold)):
            insertVaribleIntoTable(Gold[i]['cena'],Gold[i]['data'],'GBP',Gbp['rates'][i]['mid'], Gbp['rates'][i]['effectiveDate'])
        for i in range(len(Gold)):
            insertVaribleIntoTable(Gold[i]['cena'],Gold[i]['data'],'EUR',Eur['rates'][i]['mid'], Eur['rates'][i]['effectiveDate'])
        print()
    # Wstawianie pobranych wartości do bazy danych

    

    

if len(argv) == 2 and argv[1] == 'indi':
    #Tworzenie połączenia z bazą
    def create_connection(conn):
        """ create a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect('db1.db')
        except Error as e:
            print(e)

        return conn

    def create_indicator(conn):
            """
            Query all rows in the tasks table
            :param conn: the Connection object
            :return:
            """
            
            cur = conn.cursor()
            cur.execute("SELECT ID, VALUE, MID FROM GOLD")
            

            rows = cur.fetchall()
            for row in rows:
                cur.execute(
                    "UPDATE GOLD SET WSKAZNIK = :wartosc_1 WHERE ID = :biezace_id",
                    {"wartosc_1": round(row[1] / row[2], 2),
                    "biezace_id": row[0]}
                )
            
              
                
                
    def main():
        database = r"db1.db"
        # create a database connection
        conn = create_connection(database)
        with conn:
            print("Wyliczam i wprowadzam do bazy wskaznik GOLD/CURRENCY")
            create_indicator(conn)


    if __name__ == '__main__':
        main() 

   




    