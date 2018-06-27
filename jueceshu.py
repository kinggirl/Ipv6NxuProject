# -*- coding: utf-8 -*-
import numpy as np
from sklearn import tree
from sklearn.metrics import precision_recall_curve
from sklearn.metrics import classification_report

# 数据读入
data = []
labels = []
with open("jueceshu_lables.csv") as ifile:
    for line in ifile:
        tokens = line.strip().split('\t')
        data.append([float(tk) for tk in tokens[:-1]])
        labels.append(tokens[-1])
x_train = np.array(data)
y_train = np.array(labels)
# 使用信息熵作为划分标准，对决策树进行训练
clf = tree.DecisionTreeClassifier(criterion='entropy')
print(clf)
clf.fit(x_train, y_train)
# 把决策树结构写入文件
with open("tree.dot", 'w') as f:
    f = tree.export_graphviz(clf, out_file=f)

# 系数反映每个特征的影响力。越大表示该特征在分类中起到的作用越大
print(clf.feature_importances_)

# 测试结果的打印
answer = clf.predict(x_train)

print(x_train)
print(answer)
print(y_train)
print(np.mean(answer == y_train))

# 准确率与召回率
precision, recall, thresholds = precision_recall_curve(y_train, clf.predict(x_train))
answer = clf.predict_proba(x_train)[:, 1]
print(classification_report(y_train, answer, target_names=['normal', 'attack']))