from pyspark import SparkContext
from numpy import array
from pyspark.mllib.clustering import KMeans, KMeansModel

############################################
#### PLEASE USE THE GIVEN PARAMETERS     ###
#### FOR TRAINING YOUR KMEANS CLUSTERING ###
#### MODEL                               ###
############################################

NUM_CLUSTERS = 4
SEED = 0
MAX_ITERATIONS = 100
INITIALIZATION_MODE = "random"

sc = SparkContext()


def get_clusters(label, data_rdd, num_clusters=NUM_CLUSTERS, max_iterations=MAX_ITERATIONS,
                 initialization_mode=INITIALIZATION_MODE, seed=SEED):
    # TODO:
    # Use the given data and the cluster pparameters to train a K-Means model
    # Find the cluster id corresponding to data point (a car)
    # Return a list of lists of the titles which belong to the same cluster
    # For example, if the output is [["Mercedes", "Audi"], ["Honda", "Hyundai"]]
    # Then "Mercedes" and "Audi" should have the same cluster id, and "Honda" and
    # "Hyundai" should have the same cluster id
    #print(data_rdd)
    clusters = KMeans.train(data_rdd, num_clusters, maxIterations=max_iterations, initializationMode=initialization_mode, seed=seed)
    predicted_zone = clusters.predict(data_rdd).collect()
    result_list = []
    for i in range (NUM_CLUSTERS):
        result_list.append([])
    for i in range (len(predicted_zone)):
        result_list[predicted_zone[i]].append(label[i])
    
    return result_list


if __name__ == "__main__":
    f = sc.textFile("dataset/cars.data")
    data_rdd = f.map(lambda line: array([float(x) for x in line.split(',')[1:]]))
    label = f.map(lambda line: line.split(',')[0]).collect()
    clusters = get_clusters(label, data_rdd)

    for cluster in clusters:
        print(','.join(cluster))
