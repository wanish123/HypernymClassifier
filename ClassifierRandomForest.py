import numpy as np
from sklearn import cross_validation
from sklearn import svm
from sklearn.ensemble import BaggingClassifier, RandomForestClassifier
from sklearn.metrics import f1_score
from sklearn.metrics import recall_score
from sklearn.metrics import precision_score



class HypernymClassifier():

    def __init__(self, X=None, Y=None):
        self.init = False
        self.clf = RandomForestClassifier(min_samples_leaf=20)

    def fit(self, X, Y):
        if X is not None and len(X)>0 and Y is not None and len(Y) > 0 :
            self.init = True
            self.clf.fit(X, Y)
        else:
            raise "X or Y are empty or None"

    # X: 2D array, containing 1 vector for prediction
    def predict(self, X):
        if not self.init:
            raise "Classifier has not been trained yet."
        if X is None or len(X) == 0:
            raise "X is empty"
        return self.clf.predict(X)

    def score(self, X, Y):
        if not self.init:
            raise "Classifier has not been trained yet."
        if X is None or len(X) == 0:
            raise "X is empty"

        return self.clf.score(X,Y)

    def perf_measure(self, y_actual, y_pred, x_test):

        TP = 0
        FP = 0
        TN = 0
        FN = 0

        tpValue = []
        fpValue = []
        tnValue = []
        fnValue = []

        for i in range(len(y_pred)):
            if y_actual[i] == y_pred[i] == 1:
                TP += 1
                tpValue.append(x_test[i])

            if y_actual[i] == y_pred[i] == 0:
                TN += 1
                tnValue.append(x_test[i])


            if y_actual[i] == 0 and y_pred[i] == 1:
                FP += 1
                fpValue.append(x_test[i])


            if y_actual[i] == 1 and y_pred[i] == 0:
                FN += 1
                fnValue.append(x_test[i])

        return TP, FP, TN, FN, tpValue, tnValue, fpValue, fnValue


def parse_input(filename):
    X = []
    Y = []
    with open(filename) as file:
        lines = file.read()
        lines = lines.split("*\n")

        # parse annotation data
        y = lines[1].replace(",", "").split(" ")
        del y[-1]
        Y = [int(x) for x in y]

        x = lines[0].split("\n")
        x = [v.replace(",","") for v in x]
        x = [ v.split(" ") for v in x]
        for v in x:
            del v[-1]
        del x[-1]

        X = [ [int(i) for i in v] for v in x]

    return X, Y



if __name__ == "__main__":
    print "Classifier RandomForest"
    test = 0.1
    print "DPMIN = 60, test_size = " , test
    clf = HypernymClassifier()

    print "Parsing input"
    X, Y = parse_input("Output_60.txt")

    X = np.array(X)
    Y = np.array(Y)

    print "Splitting to train and test"
    X_train, X_test, Y_train, Y_test = cross_validation.train_test_split(X, Y, test_size = test, random_state = 0)

    print "size(X_train)", len(X_train)
    print "size(Y_train)", len(Y_train)
    print "size(X_test)", len(X_test)
    print "size(Y_test)", len(Y_test)
    print "Training"

    clf.fit(X_train,Y_train)

    print "Predicting"
    Y_pred= clf.predict(X_test)



    TP, FP, TN, FN, tpValue, tnValue, fpValue, fnValue = clf.perf_measure(Y_test, Y_pred, X_test)
    print 'Testing {0} noun pairs'.format(len(Y_test))
    print "TP = ", TP, " FP = ", FP, " TN = ", TN, " FN = ", FN

    fpValue = ''.join(str(e).replace(" ", "") for e in fpValue)
    print "FP = ", fpValue

    print 'Recall = ' ,recall_score(Y_test, Y_pred)
    print 'Precision = ', precision_score(Y_test, Y_pred)
    print 'F measure = ', f1_score(Y_test, Y_pred, average='binary')
