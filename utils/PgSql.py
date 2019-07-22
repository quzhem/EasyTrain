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

    def select(self, sqlCode: object, page: object = 1, pageSize: object = 10) -> object:
        offset = 0 if page <= 1 else (page - 1) * pageSize
        sqlCode = "%s offset %s limit %s " % (sqlCode, offset, pageSize)
        return self.execute(sqlCode)

    def insert(self, sqlCode, vars=None):
        self.executeWithTransaction(sqlCode, vars)

    def update(self, sqlCode, vars=None):
        self.executeWithTransaction(sqlCode, vars)

    def delete(self, sqlCode, vars=None):
        self.executeWithTransaction(sqlCode, vars)

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
            self._cursor.execute(sql_code + " RETURNING " + field)
        except Exception as e:
            print(e)
            self._conn.rollback()
            self._cursor.execute(sql_code + " RETURNING " + field)
        self.conn.commit()

        return self._cursor.fetchone()

    def executeWithTransaction(self, sqlCode, vars=None):
        try:
            self.execute(sqlCode, vars)
            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            Log.v(e)

    def execute(self, sqlCode, vars=None):
        self._cursor.execute(sqlCode, vars)
        if (self._cursor.description is None):
            return
        data = self._cursor.fetchall()
        index = self._cursor.description
        result = []
        for res in data:
            row = {}
            for i in range(len(index) - 1):
                row[index[i][0]] = res[i]
            result.append(row)
        return result

    def __del__(self):
        Log.v("关闭数据库")
        self.close()


def main():
    pgsql = PgSql("192.168.6.85", "investment", "postgres", "kh^%kfdk3ff23")
    # pgsql = PgSql("localhost", "postgres", "postgres", "123456")
    pgsql.delete("delete from paas_mt_insight_profit where id=%s", ('ceshi',))
    pgsql.insert("insert into paas_mt_insight_profit (id,tenant_id,object_describe_api_name) values('ceshi','1','1')")
    rows = pgsql.select("SELECT * from paas_mt_insight_profit where id='%s'" % 'ceshi')
    Log.v(rows)
    pgsql.update("update paas_mt_insight_profit set tenant_id='%s'" % 2)
    rows = pgsql.select("SELECT * from paas_mt_insight_profit where id='%s'" % 'ceshi')
    Log.v(rows)
    assert rows[0]['tenant_id'] == '2', 'ss'
    pgsql.delete("delete from paas_mt_insight_profit where id=%s", ('ceshi',))


if __name__ == '__main__':
    main()
