# pip install psycopg2-binary
import psycopg2
from utils.Log import Log
import threading


class PgSql:
    conn = None
    cur = None

    @staticmethod
    def init(host, database, user, password, port="5432"):
        PgSql.conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        Log.v("Opened database successfully")
        PgSql.cur = PgSql.conn.cursor()

    def select(sql):
        cur = PgSql.cur
        cur.execute(sql)
        rows = cur.fetchall()
        for row in rows:
            Log.v(row)

    @staticmethod
    def close():
        PgSql.cur.close()
        PgSql.conn.close()


def main():
    PgSql.init("localhost", "postgres", "postgres", "123456")
    PgSql.select('SELECT 1,2,3 ')
    PgSql.close()


if __name__ == '__main__':
    main()
