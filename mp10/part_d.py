from numpy import array
from pyspark.mllib.tree import RandomForest
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint

sc = SparkContext()  


def predict(training_data, test_data):
    # TODO: Train random forest classifier from given data
    # Result should be an RDD with the prediction of the random forest for each
    # test data point
    
    model = RandomForest.trainClassifier(training_data, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=2, featureSubsetStrategy = "auto",
                                     impurity='gini', maxDepth = 30, maxBins=32, seed = 12345)
    result = model.predict(test_data)
    return result


if __name__ == "__main__":
    raw_training_data = sc.textFile("dataset/training.data")
    raw_test_data = sc.textFile("dataset/test-features.data")

    # Hint: Look at the format of data required by the random forest classifier
    # Hint 2: map() can be used to process each line in raw_training_data and
    # raw_test_data
    training_data = raw_training_data.map(lambda line: LabeledPoint(int(line.split(',')[8]), array([float(x) for x in line.split(',')[0:8]])))

    # TODO: Parse RDD from raw test data
    # Hint: Look at the data format required by the random forest classifier
    test_data = raw_test_data.map(lambda line: array([float(x) for x in line.split(',')]))

    predictions = predict(training_data, test_data)

    # You can take a look at dataset/test-labels.data to see if your
    # predictions were right
    for pred in predictions.collect():
        print(int(pred))
