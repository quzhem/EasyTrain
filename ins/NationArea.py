class NationArea:
    pgSql = None
    tableName = "nation_area"

    def __init__(self, pgsql) -> None:
        self.pgSql = pgsql

    def queryByLabel(self, apiName, label, parent=None):
        andSql = " where api_name='%s' and label='%s'" % (apiName, label)
        if (parent != None):
            andSql += " and parent ='%s' " % parent

        result = self.pgSql.select("select * from %s" % self.tableName + andSql)
        if (result == None or len(result) == 0):
            return None
        return result[0]

    def queryProvinceByLabel(self, label):
        return self.queryByLabel('province', label)

    def queryCityByLabel(self, label, parent):
        return self.queryByLabel('city', label, parent)


if __name__ == '__main__':
    pass
