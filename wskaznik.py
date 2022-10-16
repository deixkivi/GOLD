import sqlite3
from sqlite3 import Error


class Wskaznik:    
    def create_connection(db_file):
        """ create a database connection to the SQLite database
            specified by the db_file
        :param db_file: database file
        :return: Connection object or None
        """
        conn = None
        try:
            conn = sqlite3.connect(db_file)
        except Error as e:
            print(e)

        return conn



    def select_all_tasks(conn):
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
                "UPDATE GOLD SET WSKAZNIK = :wartosc_1 WHERE id = :biezace_id",
                {"wartosc_1": row["VALUE"] + row["MID"],
                "biezace_id": row["ID"],}
            )

            

def main():
    database = r"db1.db"
    # create a database connection
    conn = Wskaznik.create_connection(database)
    with conn:
        print("1. Query all GOLD")
        Wskaznik.select_all_tasks(conn)


if __name__ == '__main__':
    main()