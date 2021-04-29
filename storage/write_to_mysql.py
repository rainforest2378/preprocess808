# -*- coding:utf-8 -*-
import traceback
import tarfile
import os
import pandas as pd
import redis   # 导入redis 模块
import time
import argparse
import threading
from queue import Queue

import pymysql
from dbutils.pooled_db import PooledDB
import csv
import sys

##创建连接池，连接MySQL数据库
def mysql_connection_pool():
    max_conn=15
    pool = PooledDB(pymysql,max_conn,host='10.10.103.148',user='root',port=3306,passwd='123456',db='metadata42')
    return pool


##获取目录中文件
def get_file_name(file_dir):
    L = []
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            # print(file)
            L.append(file)
    return L


##删除文件
def rm_file(path):
    mypath, myfile = os.path.split(path)
    if os.path.exists(path):  # 如果文件存在
        # 删除文件，可使用以下两种方法。
        os.remove(path)
        # os.unlink(path)

        print('remove %s' % myfile)
    else:
        pass
        #print('no such file:%s' % myfile)  # 则返回文件不存在

##解压文件夹
def un_tar(file_names, file_path,conn,cursor):
    file_name = file_names
    tar = tarfile.open(file_path + "//" + file_name)


    if os.path.isdir(file_name):
        pass
    else:
        pass

    names = tar.getnames()
  
    for name in names:
        tar.extract(name, file_path)
        path = file_path + "/" + name
        tcp_name=name[4:]
        #处理解压后的数据文件
        handle_file(path,tcp_name,conn,cursor)
        rm_file(path)


##ip字符串转int类型存储
ip_to_int = lambda x:sum([256**j*int(i) for j,i in enumerate(x.split('.')[::-1])])
        

# 处理解压后的数据文件
def handle_file(path,tcp_name,conn,cursor):
    count = 0
    k=0
    #data_list包含24个list（代表24个小时），每个list存放对应时间的数据
    data_list=[[] for i in range(24)]
    #start=time.time()
    with open(path, "rb") as f:  # 打开文件
    #读取文件
        l = f.readlines()
        l_num = len(l)
        start=time.time()
        print(l_num)
        for data_o in l:
            count+=1
            try:
            
                data = data_o.decode().split("|")
            except:
                #print(data)
                continue
            
            version=int(data[0],16)
            ip_type=int(data[1],16)
            msgtype=int(data[2],16)
            magic_number=int(data[3],16)
            ab=time.localtime(int(data[4]))
            #csv_id=ab.tm_hour
            timest=time.strftime("%Y-%m-%d %H:%M:%S",ab)
            pkt_size=int(data[5])
            dev_ip=ip_to_int(data[6])
            vendor=data[7]
            sip=ip_to_int(data[8])
            dip=ip_to_int(data[9])
            sport=int(data[10])
            dport=int(data[11])
            protocol_id=data[12]
            app_id=data[13]
            app_version=data[14]
            dev_id=data[15]
            sid=data[16]
            
            #首包时间
            a=time.localtime(int(data[17]))
            first_time=time.strftime("%Y-%m-%d %H:%M:%S",a)
            #尾包时间            
            b=time.localtime(int(data[18]))
            csv_id=b.tm_hour
            last_time=time.strftime("%Y-%m-%d %H:%M:%S",b)
            
            tx_pkts=int(data[19])
            tx_bytes=int(data[20])
            rx_pkts=int(data[21])
            rx_bytes=int(data[22])
            tcp_end=int(data[23],16)
            tcp_flag=data[24]
            header_histogram=data[25]
            payload_histogram=data[26]
            tcp_flag_histogram=data[27]
            tcp_pkts_len=data[28]
            tcp_pkts_time=data[29]
            tcp_payload_bd=data[30]
            octet_deltacount_from_Total_len=data[31]
            flow_first_pkt_time=data[32]
            flow_last_pkt_time=data[33][:-1]
            name=int(tcp_name)
            
            tuple_data=[version,ip_type,msgtype,magic_number,timest,pkt_size,dev_ip,vendor,sip,dip,sport,dport,protocol_id,app_id,app_version,dev_id,sid,first_time,last_time,tx_pkts,tx_bytes,rx_pkts,rx_bytes,tcp_end,tcp_flag,header_histogram,payload_histogram,tcp_flag_histogram,tcp_pkts_len,tcp_pkts_time,tcp_payload_bd,octet_deltacount_from_Total_len,flow_first_pkt_time,flow_last_pkt_time,name]
            data_list[csv_id].append(tuple_data)
            

            #df=pd.DataFrame(data_list)
            k+=1
            #如果数据读取完毕
            if count==l_num :
            #将data_list写入csv文件，再将csv文件load进MySQL
                for i in range(24):
                    fn="/var/lib/mysql-files/46/%stest%s.csv"%(tcp_name,str(i))
                    #print(fn)
                    with open(fn,"w") as datacsv:
                        csvwriter = csv.writer(datacsv)
                        csvwriter.writerows(data_list[i])
               # sql="LOAD DATA INFILE '/var/lib/mysql-files/test.csv' INTO TABLE test FIELDS TERMINATED BY ',' optionally enclosed by '"' escaped by '"' lines terminated by '\n\r' "
                        sql ="load data infile '%s' into table tcp%s fields terminated by ',' lines terminated by '\n' (version,ip_type,msgtype,magic_number,timestamp,pkt_size,dev_ip,vendor,sip,dip,sport,dport,protocol_id,app_id,app_version,dev_id,sid,first_time,last_time,tx_pkts,tx_bytes,rx_pkts,rx_bytes,tcp_end,tcp_flag,header_histogram,payload_histogram,tcp_flag_histogram,tcp_pkts_len,tcp_pkts_time,tcp_payload_bd,octet_deltacount_from_Total_len,flow_first_pkt_time,flow_last_pkt_time,name)"%(fn,str(i))
                        run222(sql,conn,cursor)
                        #data_list[i]=[]
                        if os.path.exists(fn):
                            os.remove(fn)
                    
                end=time.time()
                print('tcp_name%s'% tcp_name,end-start)
                break
             
    print(count)

#该函数执行将数据批量导入数据库
def run222(sql,conn,cursor):
    #批量插入方法
    #s = time.time()
    #data_list = [(i,) for i in range(10000)]
    #sql = "insert into test(version,ip_type,msgtype,magic_number,timestamp,pkt_size,dev_ip,vendor,sip,dip,sport,dport,protocol_id,app_id,app_version,dev_id,sid,first_time,last_time,tx_pkts,tx_bytes,rx_pkts,rx_bytes,tcp_end,tcp_flag,header_histogram,payload_histogram,tcp_flag_histogram,tcp_pkts_len,tcp_pkts_time,tcp_payload_bd,octet_deltacount_from_Total_len,flow_first_pkt_time,flow_last_pkt_time,name) VALUE (%s, %s, %s, %s, %s ,%s, %s, %s, %s, %s, %s, %s ,%s, %s, %s, %s, %s, %s, %s ,%s, %s, %s, %s, %s, %s, %s ,%s, %s, %s, %s, %s, %s, %s ,%s, %s)"
    try:
        #total_count = cursor.executemany(sql, data_list)
        total_count=cursor.execute(sql)
        conn.commit()
    except (Exception, BaseException) as e:
        print('{:*^60}'.format('直接打印出e, 输出错误具体原因'))
        print(e)
        print('{:*^60}'.format('使用repr打印出e, 带有错误类型'))
        print(repr(e))
        print('{:*^60}'.format('使用traceback的format_exc可以输出错误具体位置'))
        exstr = traceback.format_exc()
        print(exstr)



#添加参数--path，表示数据存放路径
parser=argparse.ArgumentParser(description='serch path')
parser.add_argument('--path',type=str,default='/data/data/2019_4_2/')
args=parser.parse_args()
global file_dir
file_dir=args.path


#conn = pymysql.connect(host='10.10.103.148', port=3306, user='root',
                  #     passwd='123456', db='metadata', charset='utf8')
#创建一个连接池



def task(My_file_name):
    #每调用一次就从线程池取出一个链接操作，完成后关闭链接
    pool= mysql_connection_pool()
    conn=pool.connection()
    cursor = conn.cursor()
           
    un_tar(My_file_name, file_dir,conn,cursor)

    cursor.close()
    conn.close()

#主程序
#启动多线程
def main():
    files = []
    files = get_file_name(file_dir)
    files=sorted(files)
    tar_list=[]
    for My_file_name in files:
        if My_file_name.find(".tar.gz") != -1:
            tar_list.append(My_file_name)
    #开始时间
    st=time.time()
    #创建队列
    q=Queue(maxsize=1)
    while tar_list:
        #获取一个压缩包
        content=tar_list.pop()
        #开启一个线程
        th= threading.Thread(target=task,args=(content,))
        #将线程放入队列
        q.put(th)
        #如果压缩包列表为空或者队列满了，就启动队列中的线程
        if (q.full()==True) or (len(tar_list))==0:
            thread_list=[]
            while q.empty() == False:
                 t= q.get()
                 thread_list.append(t)
                 t.start()
            for t in thread_list:
                t.join()
    #结束时间
    et=time.time()
    print('total_time',et-st)

main()
