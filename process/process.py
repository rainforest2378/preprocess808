import os
import linecache
import redis
import subprocess
import time
import threading

start_time=time.time()

#提取特征
def fe1( word2, word3, fn):
    file_handle=open(fn,'r')

    l=file_handle.readlines()
    total_t=len(l)
    total_h=0
    total_p=0
    total_hp=0
    num1=[]
    line_num=0
    for i in l:
        line_num+=1
        if word2 in i:
        
            total_h+=1
        if word3 in i:
            total_p+=1
            if word2 in i:
                total_hp+=1

    print(total_t,total_h,total_p,total_hp)

    return total_t,total_h,total_p,total_hp

#提取特征
def fe2(word2s, word2d, word3s, word3d, fn):
    file_handle = open(fn, 'r')

    l = file_handle.readlines()
    total_t = len(l)
    total_h = 0
    total_p = 0
    total_hp = 0
    total_sp = 0
    total_sh = 0
    total_h_sp = 0
    num1 = []

    for i in l:
        x = i.split('|')
        sh = x[0]
        dh = x[1]
        sp = x[2]
        dp = x[3]

        if word2s == sh and word2d != dh:
            total_sh += 1
        if word2s == sh and word2d == dh:
            total_h += 1
            if word3s == sp and word3d != dp:
                total_h_sp += 1


    return total_sh, total_h_sp

def pro(l,s):
    for i in l:
        # key_num = search_sid(f, i)
        ind = l.index(i)
        data_origin=s[ind]
        data=data_origin
        #for ss in data:
            #r2.rpush(i,ss)

        timea = data[5]
        timeArray = time.strptime(timea, "%Y-%m-%d %H:%M:%S")
        # 转为时间戳
        timeStamp = int(time.mktime(timeArray))
        sip = data[9]
        dip = data[10]
        sp = data[11]
        dp = data[12]
        #sid = data[16]

        #获取要匹配的字符串
        word2s = sip
        word2d = dip
        word3s = sp
        word3d = dp

        word1 = time
        word2 = sip + '|' + dip
        word3 = sp + '|' + dp

        #数据文件路径
        fn='/home/fyl/code/process42'+'/'+str(timeStamp)
        try:
            total_t, total_h, total_p,total_hp = fe1( word2, word3, fn)
            total_sh, total_h_sp = fe2(word2s, word2d, word3s, word3d, fn)
        except:
            print(i)
            print(data)
            continue

        same_sip_diff_dip = total_sh
        same_host_same_sp_diff_dp = total_h_sp
        total_t-=1
        total_h-=1
        total_p-=1
        total_hp-=1
        if total_t==0:
            same_host=0
            same_host_rate=0
            same_host_srv_rate=0
              
            same_srv=0
            same_srv_rate=0
            srv_diff_host_rate=0
            new =  str(same_host) + '|' + str(same_host_rate) + '|' + str(same_host_srv_rate) + '|' + str(
            same_srv) + '|' + str(same_srv_rate) + '|' + str(srv_diff_host_rate)+'|'+str(same_sip_diff_dip) + '|' + str(same_host_same_sp_diff_dp)

            print(new)
            r2.rpush(i, new)
            continue

        if total_h==0:
            same_host=0
            same_host_rate=0
            same_host_srv_rate=0
            
            if total_p==0:
                same_srv=0
                same_srv_rate=0
                srv_diff_host_rate=1
            else:
                same_srv = total_p
                same_srv_rate = total_p / total_t
                srv_diff_host_rate = 1 - total_hp / total_p
            

            new = str(same_host) + '|' + str(same_host_rate) + '|' + str(same_host_srv_rate) + '|' + str(
            same_srv) + '|' + str(same_srv_rate) + '|' + str(srv_diff_host_rate)+'|'+str(same_sip_diff_dip) + '|' + str(same_host_same_sp_diff_dp)

            print(new)
            r2.rpush(i, new)

            continue
        
                
         
        same_host = total_h
        same_host_rate = total_h / total_t
        same_host_srv_rate = total_hp / total_h

        same_host_rate = round(same_host_rate, 6)
        same_host_srv_rate = round(same_host_srv_rate, 3)

        if total_p==0:
            same_srv=0
            same_srv_rate=0
            srv_diff_host_rate=1.0
            new = str(same_host) + '|' + str(same_host_rate) + '|' + str(same_host_srv_rate) + '|' + str(
            same_srv) + '|' + str(same_srv_rate) + '|' + str(srv_diff_host_rate)+'|'+str(same_sip_diff_dip) + '|' + str(same_host_same_sp_diff_dp)

            print(new)
            r2.rpush(i, new)
            continue         


        same_srv = total_p
        same_srv_rate = total_p / total_t
        srv_diff_host_rate = 1 - total_hp / total_p

        same_srv_rate = round(same_srv_rate, 6)

        new = str(same_host) + '|' + str(same_host_rate) + '|' + str(same_host_srv_rate) + '|' + str(
            same_srv) + '|' + str(same_srv_rate) + '|' + str(srv_diff_host_rate)+'|'+ str(same_sip_diff_dip) + '|' + str(same_host_same_sp_diff_dp)
        print(new)
        r2.rpush(i, new)



pool1 = redis.ConnectionPool(host='10.10.103.82', port=32680,db=2,password='redis', decode_responses=True)
r1 = redis.Redis(connection_pool=pool1)

pool2 = redis.ConnectionPool(host='10.10.103.82', port=32680,db=12,password='redis', decode_responses=True)
r2 = redis.Redis(connection_pool=pool2)

keys = r1.keys()
l = []
s = []
count = 0
for key in keys:
    count += 1
    sss = r1.lrange(key,0,-1)
    l.append(key)
    s.append(sss)
    print(len(s))

pro(l,s)

print(time.time()-start_time)

