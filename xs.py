"""
目标：
获得雪球网上关注比特币基金的所有大v的关注关系

方法分为四步：
爬取雪球网上关注比特币基金的所有用户的id，并存入数据库中；
对数据库中的id依次访问，爬取用户名和粉丝数，并存入数据库中；
筛选粉丝数达到一万以上的大v，爬取每个大v的关注列表，并存入数据库；
获取符合条件的大v中的关注关系。
"""

from fake_useragent import UserAgent
import requests
import json
import pyodbc
from retry import retry
import time
import pandas as pd
import numpy as np


class XueqiuSpider():
    def __init__(self):
        self.ua = UserAgent()
        self.headers = {'Connection': 'close', 'User-Agent': self.ua.random,
            'Cookie': "Hm_lvt_1db88642e346389874251b5a1eded6e3=1622561128; device_id=d292f6921e8abcfe6c719142544fc998; remember=1; xq_a_token=37dc1307972d5c805912f48b75d48c7bd8f0e52a; xqat=37dc1307972d5c805912f48b75d48c7bd8f0e52a; xq_id_token=eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjYzNjAxOTMxNTYsImlzcyI6InVjIiwiZXhwIjoxNjI1MjAzMzU1LCJjdG0iOjE2MjI2MTEzNTU1OTAsImNpZCI6ImQ5ZDBuNEFadXAifQ.hmCOVW7W6eSzSmubFTmuuSZTgwfgS6vyvrSr5KV14qN1N0MTYKlKGLMn3cqallqGyKbkDaja3mBhJ2rTznRqGTC6JoSQv6BGmvE_QIU-sEbPE6SpjD7l1yquzeEGSpc8c1VX-L_X7zujDyhhs8bnZbAjvmbNagWH5VNJ3qI7fpmegic0xwbmZlgjQfrBaOqfKzBluGNhnkX9Qt_FarhPprGa4Up_oE5CrUJTtnNl5r9SzzIkwZlVpvxNJRl8EvCm1IXXSv0jBM4jJxZZixaEBVK_S5jEoHzc6U320oWp26-9ynsT9YlvLycXPOU3sRaFC1qV2k_6NKwjvvm82kEMWg; xq_r_token=8d66d1209395e7b9196063df2a69a8f5b9ab4d14; xq_is_login=1; u=6360193156; s=ds18p0acqd; bid=3c85fd253904dbf96d76362f382aac26_kpf18pbw; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1622614200"}
        self.proxies = {'http': 'http://88.198.50.103:8080',
                   'http': 'http://47.89.40.231:3128',
                   'http': 'http://27.148.248.203:80',
                   'http': 'http://180.97.34.35:80',
                   'http': 'http://183.47.237.251:80'}
        self.all_id = []  # 所有用户的id列表
        self.v_id = []  # 所有大v的id列表
        self.gz_id = []  # 所有被大v关注的用户的id列表


    # 第一步，爬取关注这个股票的用户id，并存入数据库中
    def get_id(self, page):
        url='https://stock.xueqiu.com/v5/stock/portfolio/stock/follower_list.json?symbol=GBTC&page='+str(page)+'&size=20&anonymous_filter=true'
        req=requests.get(url,headers=self.headers)
        id_list=json.loads(req.text)['data']['followers']
        for i in id_list:
            self.all_id.append(i)
        print("第"+str(page)+"页已完成")

    def wr_sql(self):
        sql = "insert into XUEQIU values(?,?,?)"  # 将获得的用户id以及用户名和粉丝数的初始值存入数据库的第一张表（XUEQIU）
        for i in self.all_id:
            cursor.execute(sql, (i, 0, ""))
        print('sql执行成功')


    # 第二步，对数据库中的id依次访问，获取该id的名字和粉丝数，并进行保存
    @retry(ConnectionError, tries=4, delay=2, jitter=1)
    def get_detail(self, id):
        url = 'https://xueqiu.com/statuses/original/show.json?user_id=' + str(id)
        try:
            req = requests.get(url, headers=self.headers, timeout=5, proxies=self.proxies)
            name = json.loads(req.text)['title'].replace("的专栏", "")
            fs = json.loads(req.text)['user']['followers_count']
            return name, fs
        except:
            return 'NULL', 0

    def update_sql(self):  # 新数据库的第一张表（XUEQIU）
        sql = "SELECT ID FROM XUEQIU"  # 获取所有id
        cursor.execute(sql)
        results = cursor.fetchall()
        num = 1
        num1 = 1
        for i in range(0, len(results)):  # 获得数据库的第一张表（XUEQIU）中每一个id对应的NAME的数值长度（默认为0）
            id = results[i][0]
            sql1 = "SELECT NAME FROM XUEQIU WHERE ID= ? "
            cursor.execute(sql1, id)
            result = cursor.fetchone()
            try:
                length = len(result[0])
            except:
                length = 0
            if result[0] == None or length == 0:  # 保证已经处理过的id不再处理，提高效率
                print(int(results[i][0]))
                name, fs = self.get_detail(int(results[i][0]))
                sql2 = "update XUEQIU set NAME= ? ,FENSI= ? where ID = ? "
                cursor.execute(sql2, name, fs, id)
                conn.commit()
                if num % 3000 == 0:   # 每处理3000条等待5秒
                    time.sleep(5)
                print("已更新" + str(num) + "条")
                num += 1
            else:
                print("未更新" + str(num1))
                num1 += 1
                continue

    # 第三步，判断大v，爬取每个大v的关注列表，并存入数据库
    def get_v_id(self):  # 获取所有大v的id,判断标准为粉丝数大于10000
        cursor.execute("SELECT ID, FENSI FROM XUEQIU")
        results = cursor.fetchall()
        for i in results:
            if i[1] >= 10000:
                self.v_id.append(i[0])
            else:
                continue
        return self.v_id

    @retry(ConnectionError, tries=4, delay=2, jitter=1)
    def get_gzlist(self, id):  # 获取每个大v的关注列表,并存入数据库
        url = 'https://xueqiu.com/friendships/groups/members.json?uid=' + str(id) + '&page=1&gid=0'
        req = requests.get(url, headers=self.headers, proxies=self.proxies)
        maxpage = json.loads(req.text)['maxPage']
        for page in range(1, maxpage + 1):
            url1 = 'https://xueqiu.com/friendships/groups/members.json?uid=' + str(id) + '&page=' + str(page) + '&gid=0'
            req1 = requests.get(url1, headers=self.headers, proxies=self.proxies)
            try:
                data = json.loads(req1.text)['users']
            except:
                print("error")
            for i in data:
                self.gz_id.append(i['id'])
            print("第" + str(page) + "页已完成")
        self.wr_sql1(id)

    def wr_sql1(self, id):  # 将结果写入数据库中的第二张表（XUEQIU2）
        sql = "insert into XUEQIU2 values(?,?)"
        for i in self.gz_id:
            cursor.execute(sql, (str(id), str(i)))
        conn.commit()
        print(str(id) + "已存入数据库")

    # 第四步，获取符合条件的大v中的关注关系
    def get_name(self, k):  # 给定id获得对应用户名（k为给定id在大v的id列表中的序号）
        all_name = []  # 所有用户的用户名列表
        all_id1 = []  # 对应的id列表
        cursor.execute("SELECT ID, NAME FROM XUEQIU")
        results = cursor.fetchall()
        for i in results:
            all_id1.append(i[0])
            all_name.append(i[1])
        for j in range(len(self.all_id1)):
            if self.v_id[k] == self.all_id1[j]:  # 若给定id等于所有id里的第j项，则返回第j项对应的用户名
                 return all_name[j]

    def judge(self, id1, id2):  # 判断id1是否关注了id2
        sql = "SELECT * FROM XUEQIU2 WHERE VID= ? and GZID= ?"
        cursor.execute(sql, id1, id2)
        data = cursor.fetchall()
        if len(data) == 0:  # 不存在关注关系则返回0，若存在则返回1
            return 0
        else:
            print(id1,id2)
            return 1

    def get_relation_matrix(self):  # 获取关系矩阵并保存
        length = len(self.v_id)
        data = np.full((length, length), -1)  # 令对角线为-1
        count = 1
        for i in range(length):
            for j in range(length):
                if i == j:
                    continue
                else:
                    data[i][j] = self.judge(self.v_id[i], self.v_id[j])
                print("第" + str(count) + "组判断完成")
                count += 1
        np.savetxt('data1.txt', data, fmt="%d")  # 将关系矩阵存入txt

    def get_relation(self, data):  # 将关系矩阵转化为关系图输入文件，并输出为csv文件
        length = len(self.v_id)
        df = pd.DataFrame(columns=['source', 'target'])
        for i in range(length):
            for j in range(length):
                if data[i][j] == 1:  # 若关系矩阵中对应位置为1，则说明存在关注关系
                    df = df.append({'source': self.get_name(i), 'target': self.get_name(j)}, ignore_index=True)  # 获取关注关系双方的用户名
                print("(" + str(i) + "," + str(j) + ")")
        df.to_csv("final.csv", encoding="utf_8_sig")


conn = pyodbc.connect(r'DRIVER={SQL Server Native Client 10.0};SERVER=localhost,1433;DATABASE=DataTest;Trusted_Connection=yes')
cursor=conn.cursor()  # 创建两张表（XUEQIU和XUEQIU2）
sql = """CREATE TABLE XUEQIU(
            ID VARCHAR(20),
            FENSI INT, 
            NAME VARCHAR(40))
            """
cursor.execute(sql)
sql1 = """CREATE TABLE XUEQIU2(
        VID CHAR(20),
        GZID CHAR(20))
        """
cursor.execute(sql1)

if __name__=='__main__':
    xs = XueqiuSpider()
    #第一步，获得id列表
    for i in range(2021):   # 2021为股票关注者页面的页码
        xs.get_id(i)
    xs.wr_sql()

    #第二步，补充具体信息
    xs.update_sql()

    #第三步，获得大v关注列表
    v_id = xs.get_v_id()

    for i in range(0, len(v_id)):
        xs.get_gzlist(v_id[i])
        print("第" + str(i + 1) + "个大v已完成")

    #第四步，获得关注关系
    xs.get_relation_matrix()
    data = np.loadtxt('data.txt', dtype=np.int)
    xs.get_relation(data)

cursor.close()
conn.commit()
conn.close()
