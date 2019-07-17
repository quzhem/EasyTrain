# pip install psycopg2-binary
import psycopg2
from utils.Log import Log
import threading


class PgSql:
    def __init__(self, host, db, user, pwd, port=5432):
        self.host = host
        self.db = db
        self.user = user
        self.pwd = pwd
        self.port = port
        self._conn = self._connect()
        self._cursor = self._conn.cursor()

    def try_except(self):
        def wrapper(*args, **kwargs):
            try:
                return self(*args, **kwargs)
            except Exception as e:
                Log.e("get error: %s" % e)

        return wrapper

    @try_except
    def _connect(self):
        return psycopg2.connect(
            database=self.db,
            user=self.user,
            password=self.pwd,
            host=self.host,
            port=self.port)

    def select(self, sqlCode, page=1, pageSize=10):
        offset = 0 if page <= 1 else page * pageSize
        sqlCode = "select * from (%s) as a offset %s limit %s " % (sqlCode, offset, pageSize)
        self.execute(sqlCode)
        return self._cursor.fetchall()

    def insert(self, sqlCode):
        self.executeWithTransaction(sqlCode)

    def update(self, sqlCode):
        self.executeWithTransaction(sqlCode)

    def delete(self, sqlCode):
        self.executeWithTransaction(sqlCode)

    def close(self):
        self._cursor.close()
        self._conn.close()

    def insertAndGetField(self, sql_code, field):
        """
        插入数据，并返回当前 field
        :param sql_code:
        :param field:
        :return:
        """
        try:
            self.cursor.execute(sql_code + " RETURNING " + field)
        except Exception as e:
            print(e)
            self._conn.rollback()
            self.cursor.execute(sql_code + " RETURNING " + field)
        self.conn.commit()

        return self.cursor.fetchone()

    def executeWithTransaction(self, sqlCode):
        try:
            self.execute(sqlCode)
            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            Log.v(e)

    def execute(self, sqlCode):
        return self._cursor.execute(sqlCode)

    def __del__(self):
        Log.v("关闭数据库")
        self.close()


def main():
    pgsql = PgSql("192.168.6.85", "investment", "postgres", "kh^%kfdk3ff23")
    # pgsql = PgSql("localhost", "postgres", "postgres", "123456")
    pgsql.insert("insert into paas_mt_insight_profit (id,tenant_id,object_describe_api_name) values('ceshi','1','1')")
    rows = pgsql.select("SELECT * from paas_mt_insight_profit where id='%s'" % 'ceshi')
    Log.v(rows)
    pgsql.update("update paas_mt_insight_profit set tenant_id='%s'" % 2)
    rows = pgsql.select("SELECT * from paas_mt_insight_profit where id='%s'" % 'ceshi')
    Log.v(rows)
    assert rows[0]['tenant_id'] == '2','ss'


if __name__ == '__main__':
    main()
