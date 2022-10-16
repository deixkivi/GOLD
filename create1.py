# Import module
import sqlite3

class Create:
    def createTable():
# Connecting to sqlite
        conn = sqlite3.connect('db1.db')
  
# Creating a cursor object using the cursor() method
        cursor = conn.cursor()
  
# Creating table
        table ="""CREATE TABLE GOLD(ID INTEGER PRIMARY KEY,
                                        VALUE int,
                                        DATE VARCHAR(255),
                                        USD VARCHAR(255),
                                        MID int,
                                        EFFECTIVEDATE VARCHAR(255),
                                        WSKAZNIK int
                                        );"""
        cursor.execute(table)
        print('Table Created!')

# Commit your changes in the database    
        conn.commit()
  
# Closing the connection
        conn.close()