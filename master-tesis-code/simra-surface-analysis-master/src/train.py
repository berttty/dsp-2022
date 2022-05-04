from SimRaKit.DataProvider import DataProvider
import pandas as pd

dataprovider = DataProvider("/Users/leonardthomas/simra-surface-analysis/data_cache")
df = dataprovider.load_compressed("features", "feature_data")


from OSMHelper import Surface
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split

surface_mapping = {}
for case in Surface:
    if case.name not in ["wood", "unhewn_cobblestone", "sett"]:
        surface_mapping[case.name] = case.value 

def transform_label(label):
    if label in surface_mapping:
        return surface_mapping[label]
    else:
        return None

df['label'] = df['label'].apply(transform_label)
df = df.dropna()

g = df.groupby('label')
df = pd.DataFrame(g.apply(lambda x: x.sample(g.size().min()).reset_index(drop=True)))

data = df[['b_X_count', 'b_Y_count', 'b_Z_count', 'b_X_mean', 'b_Y_mean',
        'b_Z_mean', 'b_X_std', 'b_Y_std', 'b_Z_std', 'b_X_min', 'b_Y_min',
        'b_Z_min', 'b_X_25%', 'b_Y_25%', 'b_Z_25%', 'b_X_50%', 'b_Y_50%',
        'b_Z_50%', 'b_X_75%', 'b_Y_75%', 'b_Z_75%', 'b_X_max', 'b_Y_max',
        'b_Z_max', 'r_X_count', 'r_Y_count', 'r_Z_count', 'r_X_mean',
        'r_Y_mean', 'r_Z_mean', 'r_X_std', 'r_Y_std', 'r_Z_std', 'r_X_min',
        'r_Y_min', 'r_Z_min', 'r_X_25%', 'r_Y_25%', 'r_Z_25%', 'r_X_50%',
        'r_Y_50%', 'r_Z_50%', 'r_X_75%', 'r_Y_75%', 'r_Z_75%', 'r_X_max',
        'r_Y_max', 'r_Z_max']].copy()
        
data = pd.DataFrame(StandardScaler().fit_transform(data.values), index=data.index, columns=data.columns)
target_attribute = df['label']

X_train, X_test, y_train, y_test = train_test_split(data, target_attribute, stratify=target_attribute)

foo = y_train
X_train = X_train.to_numpy()
X_test = X_test.to_numpy()
y_train = y_train.to_numpy()
y_test = y_test.to_numpy()

print(X_train.shape)
print(X_test.shape)
print(y_train.shape)
print(y_test.shape)

foo.hist()

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.datasets import make_moons, make_circles, make_classification
from sklearn.neural_network import MLPClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.gaussian_process import GaussianProcessClassifier
from sklearn.gaussian_process.kernels import RBF
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis
from sklearn.metrics import confusion_matrix
from sklearn.metrics import classification_report

h = .02  # step size in the mesh

names = [
        "Nearest Neighbors",
        "Linear SVM",
        "RBF SVM",
         "Gaussian Process",
         "Decision Tree", "Random Forest", "Neural Net", "AdaBoost",
         "Naive Bayes", "QDA"]

classifiers = [
    KNeighborsClassifier(3),
    SVC(kernel="linear", C=0.025),
    SVC(gamma=2, C=1),
    GaussianProcessClassifier(1.0 * RBF(1.0)),
    DecisionTreeClassifier(max_depth=5),
    RandomForestClassifier(max_depth=5, n_estimators=10, max_features=1),
    MLPClassifier(alpha=1, max_iter=1000),
    AdaBoostClassifier(),
    GaussianNB(),
    QuadraticDiscriminantAnalysis()]

# iterate over classifiers
for name, clf in zip(names, classifiers):
    clf.fit(X_train, y_train)
    score = clf.score(X_test, y_test)
    print(name, score)

    y_pred = clf.predict(X_test)
    target_names = list(surface_mapping.keys())
    print(classification_report(y_test, y_pred, target_names=target_names))
