import pymysql
import csv
import pandas as pd

# 读取数据
def load_data_from_mysql(data):
    # 连接到mysql数据库,host连接数据库 user 用户名 password 密码 db数据库名称 charset 数据库编码格式
    conn = pymysql.connect(host='34.87.126.248',
                           port=3307,
                           user='wucong',
                           password='D0xTByCZdnzrnx8Q',
                           db='ethereum',
                           charset='utf8')
    # 创建游标对象
    cursor = conn.cursor()   # cursor当前的程序到数据之间连接管道
    # 组装sql语句 需要查询的MySQL语句
    filename = 'token_transfers'
    sql = "select token_address,from_address,to_address,value,block_timestamp from " + filename + " where token_address='"+ data +\
          "' and block_timestamp > '2019-01-01 00:00:00' and block_timestamp < '2023-01-01 00:00:00' and value!='0' limit 200 "
    # 执行sql语句
    cursor.execute(sql)

    # 获取数据库表中列的参数
    fields = cursor.description
    head = []
    # 创建文件对象
    f = open('data/token_1/'+data+'.csv','w',encoding='utf-8',newline="")
    # 基于文件对象构建csv写入对象
    csv_writer = csv.writer(f)

    # 获取数据库中表头
    for field in fields:
        head.append(field[0])
    # print(head)
    # 构建列表头
    csv_writer.writerow(head)

    # 获取所有数据
    all = cursor.fetchall()
    # 逐条输出获取到的数据类型及数据
    for each in all:
        print(type(each),each)
        csv_writer.writerow(each)

    # 关闭所有的连接
    # 关闭文件
    f.close()
    # 关闭游标
    cursor.close()
    # 关闭数据库
    conn.close()
if __name__=="__main__":
    data = pd.read_csv('data/token_1.csv')
    token_address = data['token_address']
    for i in range(0,len(token_address)):
        print(i)
        token = token_address[i]
        load_data_from_mysql(token)
