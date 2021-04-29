import redis  # 导入redis 模块

pool = redis.ConnectionPool(host='10.10.103.82', db=11, port=32680, password='redis', decode_responses=True)
r = redis.Redis(connection_pool=pool)

keys = r.keys()
lnum = len(keys)
l = []
count = 0
for key in keys:
    count += 1
    if count <= lnum:

        data = r.lrange(key, 0, -1)
        l.append(data)
        # print(data)
    else:
        break
print(len(l))

import pandas as pd

famous = pd.read_csv(r'C:\Users\fanyu\Desktop\5G\famous.csv', header=None)
registered = pd.read_csv(r'C:\Users\fanyu\Desktop\5G\registered.csv', header=None)
unregistered = pd.read_csv(r'C:\Users\fanyu\Desktop\5G\unregistered.csv', header=None)
famous_port = famous.iloc[:, 0].tolist()
registered_port = registered.iloc[:, 0].tolist()
unregistered_port = unregistered.iloc[:, 0].tolist()

import csv

header = ['sid', 'protocal', 'interval_time', 'tx_pkts', 'tx_bytes', 'rx_pkts', 'rx_bytes', 'tx_avg', 'rx_avg',
          'tcp_end',
          'same_host', 'same_host_rate', 'same_host_srv_rate',
          'same_srv', 'same_srv_rate', 'srv_diff_host_rate', 'same_sip_diff_dip', 'same_host_same_sp_diff_dp',
          'sport_is_famous', 'dport_is_famous', 'sport_is_registered', 'dport_is_registered', 'sport_is_unregistered',
          'dport_is_unregistered']
num_list = ['num1', 'num2', 'num3', 'num4', 'num5', 'num6', 'num7', 'num8', 'num9', 'rnum1', 'rnum2', 'rnum3', 'rnum4',
            'rnum5', 'rnum6', 'rnum7', 'rnum8', 'rnum9', 'label', 'discription']

header.extend(num_list)
data_all = []

#csv路径
csv_file = 'D://a//huatu//tcp42_event.csv'
for i in l:
    data_list = []
    data = i

    sport = int(data[11])
    dport = int(data[12])
    if sport in famous_port:
        sport_is_famous = 1
    else:
        sport_is_famous = 0
    if sport in registered_port:
        sport_is_registered = 1
    else:
        sport_is_registered = 0
    if sport in unregistered_port:
        sport_is_unregistered = 1
    else:
        sport_is_unregistered = 0

    if dport in famous_port:
        dport_is_famous = 1
    else:
        dport_is_famous = 0
    if dport in registered_port:
        dport_is_registered = 1
    else:
        dport_is_registered = 0
    if dport in unregistered_port:
        dport_is_unregistered = 1
    else:
        dport_is_unregistered = 0

    protocal = data[13]
    if int(data[20]) == 0:
        tx_avg = 0.0
    else:
        tx_avg = int(data[21]) / int(data[20])

    if int(data[22]) == 0:
        rx_avg = 0.0
    else:
        rx_avg = int(data[23]) / int(data[22])

    st = int(data[33].split('-')[0])
    et = int(data[34].split('-')[0])

    sid = data[17]
    data_list.append(sid)
    data_list.append(protocal)
    data_list.append(et - st)
    tx_pkts = data[20]
    tx_bytes = data[21]
    data_list.append(int(tx_pkts))
    data_list.append(int(tx_bytes))
    data_list.append(int(data[22]))
    data_list.append(int(data[23]))
    data_list.append(tx_avg)
    data_list.append(rx_avg)
    data_list.append(int(data[24]))

    try:
        fe = data[-1]
        fe_list = fe.split('|')
        for ff in fe_list:
            data_list.append(float(ff))

    except:
        print(data)
        continue

    list_is = [sport_is_famous, dport_is_famous, sport_is_registered, dport_is_registered, sport_is_unregistered,
               dport_is_unregistered]

    data_list.extend(list_is)

    v3 = data[28].split('-')
    try:
        if v3[0] != '':
            l1 = v3[0].split(';')
        else:
            l1 = [0 for i in range(9)]
    except:
        print(i)

    try:
        if v3[1] != '':
            l1 = v3[1].split(';')
        else:
            l1 = [0 for i in range(9)]
    except:
        print(i)
        continue
    list_num = []
    for i in l1:
        list_num.append(int(i))
    for i in l2:
        list_num.append(int(i))

    data_list.extend(list_num)
    label = data[36]
    dis = data[37]
    data_list.append(label)
    data_list.append(dis)

    data_all.append(data_list)

with open(csv_file, 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(header)
    writer.writerows(data_all)



