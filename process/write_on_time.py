# -*- coding:utf-8 -*-
import tarfile
import os
import pandas as pd
import redis
#该脚本将数据文件按照时间戳重新整理，方便后续进行特征提取

def get_file_name(file_dir):
    L = []
    for root, dirs, files in os.walk(file_dir):
        for file in files:
            L.append(file)
    return L


# 丢弃
def rm_file(path):
    mypath, myfile = os.path.split(path)
    if os.path.exists(path):  # 如果文件存在
        # 删除文件，可使用以下两种方法。
        os.remove(path)
        # os.unlink(path)

       # print('remove %s' % myfile)
    else:
        pass
        print('no such file:%s' % myfile)  # 则返回文件不存在

##解压
def un_tar(file_names, file_path):
    file_name = file_names
    tar = tarfile.open(file_path + "//" + file_name)
    names = tar.getnames()
    #
    for name in names:
        tar.extract(name, file_path)
        path = file_path + "/" + name
        path2=file_path+"//"+file_name
        #print('extract file complete %s' % path)

        handle_file(path)
        rm_file(path)



# 处理
def handle_file(path):
    count = 0

    with open(path, "rb") as f:  # 打开文件
        l = f.readlines()
        #lnum = len(l)/10
        for data in l:
            count+=1
            #d = data.decode().split("|")
            try:
                d = data.decode().split("|")
            except:
                #print(data)
                continue
            

            #key = d[16]
            time=d[4]
            sip=d[8]
            dip=d[9]
            sport=d[10]
            dport=d[11]
            sid=d[16]
            first_time=d[17]
            last_time=d[18]
            
            record=sip+"|"+dip+"|"+sport+"|"+dport+"|"+sid+"\n"
            create(time,record)

#创建文件
def create(str,x):
    #文件保存的目录
    path='/data/data/process/process42'
    filename=path+'/'+str
    if not os.path.isfile(filename):  # 无文件时创建
        fd = open(filename, mode="w", encoding="utf-8")
        fd.close()
    else:
        pass
    file_handle = open(filename, mode='a+')#注意是a+，不然会覆盖
    file_handle.write(x)


import argparse
parser=argparse.ArgumentParser(description='serch path')
parser.add_argument('--path',type=str,default='/data/data/2019_4_2')
#parser.add_argument('--right',type=int)
#parser.add_argument('--left',type=int)
args=parser.parse_args()
file_dir=args.path
#right=args.right
#left=args.left


files = []
files = get_file_name(file_dir)
files.sort()
print(len(files))
for My_file_name in files:
    if My_file_name.find(".tar.gz") != -1:
        print(My_file_name)
        un_tar(My_file_name, file_dir)

    else:
        pass




