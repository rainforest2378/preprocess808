# -*- coding:utf-8 -*-
import tarfile
import os
import pandas as pd
import redis   # 导入redis 模块
import traceback

import threading
from queue import Queue

import pymysql
from dbutils.pooled_db import PooledDB
import csv
import sys
from datetime import datetime

#ip到int和int到ip的转换
int_to_ip = lambda x: '.'.join([str(int(x/(256**i)%256)) for i in range(3,-1,-1)])
ip_to_int = lambda x:sum([256**j*int(i) for j,i in enumerate(x.split('.')[::-1])])

#读取事件xlsx
event_path="/home/fyl/task_two/47.xlsx"
df=pd.read_excel(event_path)
#提取时间戳，事件类型，事件描述，ip端口等信息
data=df.iloc[:,[1,3,4,6,7,8,9,13]].values
output1 = []
output2 = []
session=[]
#print("----------------")

#将提取的信息保存到output
for x in data:
    if x[7] not in session:
        session.append(x[7])
        dip=x[4].split(',')
        if len(dip)==1:
            sip=x[3]
            dip=x[4]
            sport=x[5]
            dport=x[6]
            d=dict()
            d={'timestamp':x[2],'sip':sip,'dip':dip,'sport':sport,'dport':dport,'session_id':x[7],'event_type':x[0],'event_dis':x[1]}
            output1.append(d)
            #output_simple1.append(ip_port)
            #print(d)
        else:
            for y in dip:
                sip=x[3]
                d=dict()
                d={'timestamp':x[2],'sip':sip,'dip':y,'session_id':x[7],'event_type':x[0],'event_dis':x[1]}
                output2.append(d)
               # output_simple2.append(ip)
               # print(d)


print("----------------------")
print(len(output1))
print('-------------------------------------------------')
print(len(output2))

#连接数据库
def mysql_connection_pool():
    max_conn=15
    pool = PooledDB(pymysql,max_conn,host='10.10.103.148',user='root',port=3306,passwd='123456',db='metedata47')
    return pool

#写入redis
def to_redis(result,event,event_dis,r):
    for i in range(len(result)):
        key=result[i][17]
        print(key)
        for y in result[i]:
            r.rpush(key,y)
        r.rpush(key,event)
        r.rpush(key,event_dis)
    
#创建redis连接池，连接redis
pool = redis.ConnectionPool(host='10.10.103.82',db=7, port=32680,password='redis', decode_responses=True)
r = redis.Redis(connection_pool=pool)

#创建Mysql连接池，连接mysql
pool= mysql_connection_pool()
conn=pool.connection()
cursor = conn.cursor()

#根据重要信息筛选Mysql数据，并写入redis
for i in range(len(output2)):
    sip=output2[i]['sip']
    dip=output2[i]['dip']
    sip=ip_to_int(sip)
    dip=ip_to_int(dip)
    event=output2[i]['event_type']
    event_dis=output2[i]['event_dis']
    ab=time.localtime(int(output2[i]['timestamp']))
    end_time=time.localtime(int(output2[i]['timestamp'])+5)
    start_time=time.localtime(int(output2[i]['timestamp'])-5)
    th=ab.tm_hour

    et=time.strftime("%Y-%m-%d %H:%M:%S",end_time)
    st=time.strftime("%Y-%m-%d %H:%M:%S",start_time)
    print(et,st,sip,dip,event)

    #选择在某一时间段里IP匹配的数据
    sql="SELECT * FROM tcp%s WHERE UNIX_TIMESTAMP(last_time) <= UNIX_TIMESTAMP('%s') and UNIX_TIMESTAMP(last_time) >= UNIX_TIMESTAMP('%s') and sip=%s and dip=%s;"%(th,et,st,sip,dip)
    cursor.execute(sql)
    #获取查询结果
    result2 = cursor.fetchall()
    result=list(result2)
    res=[]
   #print(result) 
    if len(result)!=0:
        for j in result:
            
            i=list(j)
            
            i[5]=i[5].strftime("%Y-%m-%d %H:%M:%S")
            i[18]=i[18].strftime("%Y-%m-%d %H:%M:%S")
            i[19]=i[19].strftime("%Y-%m-%d %H:%M:%S")

            i[7]=int_to_ip(i[7])
            i[9]=int_to_ip(i[9])
            i[10]=int_to_ip(i[10])
            print(i)
            res.append(i)
        to_redis(res,event,event_dis,r)

for i in range(len(output1)):
    sip=output1[i]['sip']
    dip=output1[i]['dip']
    event=output1[i]['event_type']
    event_dis=output1[i]['event_dis']
    sip=ip_to_int(sip)
    dip=ip_to_int(dip)
    sport=output1[i]['sport']
    dport=output1[i]['dport']
    ab=time.localtime(int(output1[i]['timestamp']))
    end_time=time.localtime(int(output1[i]['timestamp'])+20)
    start_time=time.localtime(int(output1[i]['timestamp'])-20)
    
    th=ab.tm_hour

    et=time.strftime("%Y-%m-%d %H:%M:%S",end_time)
    st=time.strftime("%Y-%m-%d %H:%M:%S",start_time)
    print(et,st,sip,dip,sport,dport,event)

    # 选择在某一时间段里IP和端口都匹配的数据
    sql="SELECT * FROM tcp%s WHERE UNIX_TIMESTAMP(last_time) <= UNIX_TIMESTAMP('%s') and UNIX_TIMESTAMP(last_time) >= UNIX_TIMESTAMP('%s') and sip=%s and dip=%s and sport=%s and dport=%s;"%(th,et,st,sip,dip,sport,dport)
    cursor.execute(sql)
    #获取查询结果
    result = cursor.fetchall()
    res=[]
    if len(result)!=0:
        for j in result:

            i=list(j)
            
            i[5]=i[5].strftime("%Y-%m-%d %H:%M:%S")
            i[18]=i[18].strftime("%Y-%m-%d %H:%M:%S")
            i[19]=i[19].strftime("%Y-%m-%d %H:%M:%S")

            i[7]=int_to_ip(i[7])
            i[9]=int_to_ip(i[9])
            i[10]=int_to_ip(i[10])

            res.append(i)
        to_redis(res,event,event_dis,r)
    




cursor.close()
conn.close()

