import matplotlib.pyplot as plt
import numpy as np
from sklearn import cross_validation
from sklearn import svm
from sklearn import tree


class HypernymClassifier():

    def __init__(self, X=None, Y=None):
        print "init"
        self.init = False
        self.clf = svm.SVC(kernel='linear')

    def fit(self, X, Y):
        if X is not None and len(X)>0 and Y is not None and len(Y) > 0 :
            self.init = True
            self.featureVectorSize = len(X[0])
            self.clf.fit(X, Y)
        else:
            raise "X or Y are empty or None"

    # X: 2d array, containing 1 vector for prediction
    def predict(self, X):
        if not self.init:
            raise "Classifier has not been trained yet."
        if X is None or len(X) == 0:
            raise "X is empty"
        # neccassary?
        if len(X[0]) != self.featureVectorSize:
            raise "vector sizes do not match"
        return self.clf.predict(X)

if __name__ == "__main__":
    clf = HypernymClassifier()
    print "here"
    X = np.array([[3,4,1],[3,4,0],[1,1,0],[7,2,1],[4,3,1],[6,7,1],[10,10,1],[-1,1,0],[-1,2,0],[-2,-2,0],[-6,-1,0]])
    Y = np.array([1,0,0,1,1,1,1,0,0,0,0])
    # X_test = np.array(
    #     [[0, 0, 0], [-2, -2, 0], [-0, -3, 0], [-5, -1, 0], [1, 1, 1], [2, 3, 0], [2, 5, 0], [8, 8, 0], [7, 9, 0],
    #      [50, 50, 1]])

    print "new"
    X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, Y, test_size = 0.1, random_state = 0)
    print "X_train"
    print X_train

    print "X"
    print X



    print "Y"
    print Y

    data_set_size = len(X_train)
    Z_train = X_train.reshape(data_set_size, -1)

    print "Z_train"
    print Z_train

    print "y_train"
    print y_train

    clf.fit(X_train,y_train)
    # clf = svm.SVC(kernel='linear').fit(np.array([Z_train]), np.array([y_train]))
    print "done"
    # print clf.score(X_test, y_test)
    # print clf

    # clf.fit(X,Y)
    # print clf.predict(X)
