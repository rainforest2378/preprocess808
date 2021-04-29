import keras
from keras.models import Sequential, Model
from keras import layers
from keras.optimizers import SGD
import pandas as pd
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pandas import Series, DataFrame
import numpy as np
from pylab import mpl
import seaborn as sns
import matplotlib.pyplot as plt
from sklearn.metrics import confusion_matrix
from sklearn.metrics import classification_report

# 加载数据
path = 'D://a//huatu//tcp_event'
benign_path = path + '//' + 'tcp.csv'
event_path = path + '//' + 'tcp42_event.csv'

benign_origin = pd.read_csv(benign_path)
event_origin = pd.read_csv(event_path)

event_origin.head(5)

# 数据预处理
from sklearn import preprocessing
from sklearn.preprocessing import OneHotEncoder
from sklearn import cross_validation
from keras.utils import np_utils
from sklearn.preprocessing import LabelEncoder
import random

benign = benign_origin.iloc[:, :42]
event = event_origin.iloc[:, :42]
benign_label = benign_origin.iloc[:, 42]
event_label = event_origin.iloc[:, 42]


random_test = [i for i in range(len(event_label))]
random.shuffle(random_test)
benign_label_onehot = np.zeros(len(benign_label))
benign_label_onehot = pd.DataFrame(benign_label_onehot)
event_keys = event_label.value_counts().to_dict().keys()
i = 1
event_label_onehot = event_label
for x in event_keys:
    ind = 0
    for e in event_label:
        if e == x:
            event_label_onehot[ind] = i
        ind += 1
    i += 1

source_xtrain = pd.concat([benign.iloc[:10000, :], event.iloc[random_test[:20000], :]])
source_xtest = pd.concat([benign.iloc[10000:11000, :], event.iloc[random_test[20000:], :]])

source_ytrain = pd.concat([benign_label_onehot[:10000], event_label_onehot[random_test[:20000]]])
source_ytest = pd.concat([benign_label_onehot[10000:11000], event_label_onehot[random_test[20000:]]])

source_xtrain = source_xtrain.fillna(method='backfill')
source_xtest = source_xtest.fillna(method='backfill')

xtrain2 = source_xtrain.drop(columns='sid', axis=1)
c = xtrain2.drop(columns='protocal', axis=1)
b = pd.DataFrame(c, dtype=np.float)
xtrain3 = preprocessing.StandardScaler().fit_transform(b)

xtest2 = source_xtest.drop(columns='sid', axis=1)
m = xtest2.drop(columns='protocal', axis=1)
n = pd.DataFrame(m, dtype=np.float)
xtest3 = preprocessing.StandardScaler().fit_transform(n)

ytrain2 = source_ytrain.values
ytest2 = source_ytest.values

source_x_train = xtrain3.reshape((xtrain3.shape[0], xtrain3.shape[1], 1))
source_x_test = xtest3.reshape((xtest3.shape[0], xtest3.shape[1], 1))
source_y_train = ytrain2
source_y_test = ytest2

encoder = LabelEncoder()
encoded_Y = encoder.fit_transform(source_y_train)
ytrain3 = np_utils.to_categorical(encoded_Y)
source_y_train = ytrain3

encoder = LabelEncoder()
encoded_Y = encoder.fit_transform(source_y_test)
ytest3 = np_utils.to_categorical(encoded_Y)
source_y_test = ytest3

print('shape of training set samples:', source_x_train.shape)
print('shape of testing set samples:', source_x_test.shape)

# 构建模型
length = 40
input_layerA = layers.Input(name='input', shape=(length, 1))

x = layers.Conv1D(64, 3, padding='same', name='conv_1', kernel_regularizer='l2')(input_layerA)
x = layers.MaxPooling1D(2)(x)
# x = layers.LeakyReLU(name='leaky_1')(x)

x = layers.Conv1D(64, 3, padding='same', name='conv_2', kernel_regularizer='l2')(x)
x = layers.MaxPooling1D(2)(x)
# x = layers.LeakyReLU(name='leaky_2')(x)

y = layers.Flatten()(x)
y = layers.Dense(128, activation="relu")(y)
y = layers.Dense(64, activation="relu")(y)
# y = layers.Dropout(0.5)(y)
final_classifier = layers.Dense(12, activation="sigmoid")(y)
tcp_cnn = Model(inputs=input_layerA, outputs=final_classifier)

print('Description of model:\n')
tcp_cnn.summary()

# 训练模型
import keras.backend as K
from keras.callbacks import TensorBoard
from keras import layers, losses
from keras.engine.topology import Layer

# 模型编译
tcp_cnn.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy'])
# 模型训练
print('Process of training:')
tcp_cnn.fit(source_x_train, source_y_train, epochs=10, validation_data=(source_x_test, source_y_test),
            callbacks=[TensorBoard(log_dir='logs_4', histogram_freq=1, )])

# 模型预测
#tcp_cnn = load_model(r'D:\a\model\task_two\visualization\tcp41_cnn.h5')
result = tcp_cnn.predict(source_x_test)

# 混淆矩阵热力图

y_pre = result
y_true = source_y_test
y_pred = [r.tolist().index(max(r.tolist())) for r in result]
y_true = [r.tolist().index(max(r.tolist())) for r in source_y_test]

C = confusion_matrix(y_true, y_pred)

if True:
    label = ['Benign', 'Info-\nDisclosure', 'Code-\nexecution ', 'XSS', 'Dir-\ntraversal', 'Injection', 'Nessus',
             'Nmap', 'Scanner', 'Webshell',
             'ZERO', 'Trojan']

    df = pd.DataFrame(C, index=label, columns=label)
    # Plot
    plt.figure(figsize=(16, 16))
    ax = sns.heatmap(df, xticklabels=df.corr().columns,
                     yticklabels=df.corr().columns, cmap='BuGn',
                     linewidths=1, annot=True)

    # Decorations
    plt.xticks(fontsize=16, family='Times New Roman')
    plt.yticks(fontsize=16, family='Times New Roman')

    plt.tight_layout()
    # plt.savefig('res/method_3.png', transparent=True, dpi=800)
    bottom, top = ax.get_ylim()
    ax.set_ylim(bottom + 0.5, top - 0.5)
    plt.show()

#保存模型
#tcp_cnn.save(r'D:\a\model\task_two\visualization\tcpx_cnn.h5')


label = ['Benign', 'Info-Disclosure', 'Code-execution ', 'XSS', 'Dir-traversal', 'Injection', 'Nessus', 'Nmap',
         'Scanner', 'Webshell',
         'ZERO', 'Trojan']
print(classification_report(y_true, y_pred, target_names=label, digits=3))
