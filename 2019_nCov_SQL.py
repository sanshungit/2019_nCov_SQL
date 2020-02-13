import mysql.connector
import time
from datetime import datetime
import json
import requests


class data_inqurey():
    def __init__(self, db_name):
        self.cnn = mysql.connector.connect(
            host='localhost',
            user="root",
            passwd='sanshun1234',
            database=db_name,
            charset='utf8mb4'
        )
        print('DATABASE %s CONNECTED!' % db_name)

    def db_close(self):
        self.cnn.close()

    def ticks_time(self):
        tab_name_p = time.strftime('ncov_%Y%m%d_P', time.localtime())
        tab_name_c = time.strftime('ncov_%Y%m%d_C', time.localtime())
        return tab_name_p, tab_name_c

    def creat_tab(self):
        mycursor = self.cnn.cursor()
        tab_name_p, tab_name_c = self.ticks_time()
        # 按照日期创建两张表，分别对应省份疫情信息与城市疫情信息
        for tab_name in (tab_name_p, tab_name_c):
            str_sql = 'CREATE TABLE ' + tab_name + '''(
                LOC_ID INT AUTO_INCREMENT PRIMARY KEY,
                LOC_NAME CHAR(10),
                CONFIRM_NUM INT(10),
                SUSPECT_NUM INT(10),
                DEAD_NUM INT(10),
                HEAL_NUM INT(10)
                )ENGINE=INNODB DEFAULT CHARSET=UTF8MB4'''
            mycursor.execute('DROP TABLE IF EXISTS %s' % (tab_name))
            mycursor.execute(str_sql)
            print('CREATE TABLE %s!' % (tab_name))
        # 创建表DALIY_NCOV用于保存每日的总数居
        str_sql = '''CREATE TABLE DALIY_NCOV(
                DATA_ID INT AUTO_INCREMENT PRIMARY KEY,
                DATE_VAL DATE,
                CONFIRM_NUM INT(10),
                SUSPECT_NUM INT(10),
                DEAD_NUM INT(10),
                HEAL_NUM INT(10)
                )ENGINE=INNODB DEFAULT CHARSET=UTF8MB4'''
        mycursor.execute('DROP TABLE IF EXISTS DALIY_NCOV')  # 删除已有的表
        mycursor.execute(str_sql)
        print('CREATE TABLE DALIY_NCOV!')
        self.cnn.commit()
        mycursor.close()

    def insert_daily(self):
        """抓取每日确诊和死亡数据"""
        mycursor = self.cnn.cursor()
        url = 'https://view.inews.qq.com/g2/getOnsInfo?name=disease_h5&callback=&_=%d' % int(time.time() * 1000)
        data = json.loads(requests.get(url=url).json()['data'])
        tmp_data = data['chinaDayList']
        str_sql = '''INSERT INTO DALIY_NCOV(
            DATE_VAL,
            CONFIRM_NUM,
            SUSPECT_NUM,
            DEAD_NUM,
            HEAL_NUM
            ) VALUES (%s,%s,%s,%s,%s)'''

        for item in tmp_data:
            month, day = item['date'].split('.')
            date_val = datetime.strptime('2020-%s-%s' % (month, day), '%Y-%m-%d')
            confirm_num = int(item['confirm'])
            suspect_num = int(item['suspect'])
            dead_num = int(item['dead'])
            heal_num = int(item['heal'])
            mycursor.execute(str_sql, [date_val, confirm_num, suspect_num, dead_num, heal_num])
        self.cnn.commit()
        mycursor.close()

    def insert_distribution(self):
        # 抓取行政区域确诊分布数据并将数据保存在MySQL中
        mycursor = self.cnn.cursor()
        tab_name_p, tab_name_c = self.ticks_time()
        str_sql_p = 'INSERT INTO ' + tab_name_p + '''(
            LOC_NAME,
            CONFIRM_NUM,
            SUSPECT_NUM,
            DEAD_NUM,
            HEAL_NUM
            ) VALUES (%s,%s,%s,%s,%s)'''
        str_sql_c = 'INSERT INTO ' + tab_name_c + '''(
            LOC_NAME,
            CONFIRM_NUM,
            SUSPECT_NUM,
            DEAD_NUM,
            HEAL_NUM
            ) VALUES (%s,%s,%s,%s,%s)'''
        data_confirm = {}
        data_suspect = {}
        data_dead = {}
        data_heal = {}
        url = 'https://view.inews.qq.com/g2/getOnsInfo?name=disease_h5&callback=&_=%d' % int(time.time() * 1000)
        tmp_data = json.loads(requests.get(url=url).json()['data'])['areaTree'][0]['children']
        for item in tmp_data:
            if item['name'] not in data_confirm:
                data_confirm.update({item['name']: 0})
                data_suspect.update({item['name']: 0})
                data_dead.update({item['name']: 0})
                data_heal.update({item['name']: 0})
            for city_data in item['children']:
                data_confirm[item['name']] += int(city_data['total']['confirm'])
                data_suspect[item['name']] += int(city_data['total']['suspect'])
                data_dead[item['name']] += int(city_data['total']['dead'])
                data_heal[item['name']] += int(city_data['total']['heal'])
                mycursor.execute(str_sql_c, [city_data['name'], int(city_data['total']['confirm']),
                                             int(city_data['total']['suspect']), int(city_data['total']['dead']),
                                             int(city_data['total']['heal'])])
            mycursor.execute(str_sql_p, [item['name'], data_confirm[item['name']], data_suspect[item['name']],
                                         data_dead[item['name']], data_heal[item['name']]])
        self.cnn.commit()
        mycursor.close()


if __name__ == '__main__':
    # 增加容错语句
    try:
        mydb = data_inqurey('2019_ncov')
        mydb.creat_tab()
        mydb.insert_distribution()
        mydb.insert_daily()
    except Exception as e:
        print('Warning: %s' % e)
    else:
        pass
    finally:
        print('COMPLETED!!!')
