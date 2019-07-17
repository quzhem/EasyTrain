from utils.Log import Log


def a1():
    str = {"artificial_person_name": "项志群", "create_time": "2019-05-23 09:06:27", "employee_nums": 10, "enterprise_nature": "内资企业",
           "establish_date": "2015-03-12 00:00:00", "fund_count": 2, "fund_scale": 2.341594766E7, "has_credit_tips": False, "has_special_tips": False,
           "honest_info": "", "id": "101000017448", "in_blacklist": False, "info_last_update_time": "2019-04-23", "institutional_website": "",
           "is_advise": False, "is_vip": True, "law_firm_name": "北京舟之同律师事务所", "law_info_status": 1, "lawyer_name": "成慧,杨东",
           "manager_name": "童虎(北京)基金管理有限公司", "manager_name_en": "Beijing Tonghu Fund Management Co., Ltd.",
           "office_address": "北京市海淀区海淀区首都体育馆南路6号北京新世纪饭店南侧写字楼8层855", "office_city": "海淀区", "office_coordinate": "39.94316173430941,116.33457943483634",
           "office_province": "北京市", "organ_code": "33544573-0", "paid_in_capital": 7.8491673E7, "primary_invest_type": 1, "reg_adr_agg": "北京市",
           "register_address": "北京市石景山区石景山区西黄新村东里2号楼1层商业10", "register_city": "石景山区", "register_date": "2015-04-10 00:00:00",
           "register_no": "P1010210", "register_province": "北京市", "special_info": "", "status": 1, "subscribed_capital": 7.8491673E7,
           "turn_in_price_pct": 1, "update_time": "2019-05-23 09:06:27", "vip_date": "2016-11-29", "vip_deputy": "项志群", "vip_type": 3}
    sql = 'CREATE TABLE "public"."paas_sync_insight_co_organization_info" (%s,' \
          'CONSTRAINT "paas_sync_insight_co_organization_info_pkey" PRIMARY KEY ("id") NOT DEFERRABLE INITIALLY IMMEDIATE)'
    result = []
    result.append('"id" varchar(24) NOT NULL COLLATE "default"')
    for (k, v) in dict(str).items():
        if (type(v) == int):
            Log.v("字段%s，类型 int" % (k))
            result.append('%s varchar(100)' % k)
            continue
        if (type(v) == float):
            Log.v("字段%s，类型 float" % (k))
            result.append('%s varchar(100)' % k)
            continue
        result.append('%s varchar(100)' % k)
    Log.v(sql % ','.join(result))


if __name__ == '__main__':
    a1()
