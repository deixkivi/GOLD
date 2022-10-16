import sqlite3

con = sqlite3.connect('asdasd.s3')
con.row_factory=sqlite3.Row
cursor = con.cursor()

#cursor.execute("drop table ble")
#cursor.execute("create table ble (kolumna_1 int, kolumna_2 int)")
cursor.execute("INSERT INTO ble VALUES (1111,5555)")
cursor.execute("SELECT * FROM ble WHERE kolumna_1 =:parametr_1", {"parametr_1": 1111})
for i in cursor.fetchall():
    print(i["kolumna_1"], i["kolumna_2"])



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
            

