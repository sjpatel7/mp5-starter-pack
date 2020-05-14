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


def get_clusters(data_rdd, num_clusters=NUM_CLUSTERS, max_iterations=MAX_ITERATIONS,
                 initialization_mode=INITIALIZATION_MODE, seed=SEED):
    # TODO:
    # Use the given data and the cluster pparameters to train a K-Means model
    # Find the cluster id corresponding to data point (a car)
    # Return a list of lists of the titles which belong to the same cluster
    # For example, if the output is [["Mercedes", "Audi"], ["Honda", "Hyundai"]]
    # Then "Mercedes" and "Audi" should have the same cluster id, and "Honda" and
    # "Hyundai" should have the same cluster id
    cars = {}
    for line in data_rdd.collect():
        point = line.split(',')
        cars[point[0]] = array([x for x in point[1:]])
    
    points = data_rdd.map(lambda line: array([float(x) for x in line.split(',')[1:]]))
    clusters = KMeans.train(points, 4, maxIterations=100, initializationMode="random")
    car_clusters = {}
    ct = 0
    for car in cars:
        features = cars[car]
        c = clusters.predict(features)
        car_clusters.setdefault(c, []).append(car)
        
    result = [[]]
    for c in car_clusters:
        cluster = []
        for car in car_clusters[c]:
            cluster.append(car)
        result.append(cluster)
    result.remove([])
    return result


if __name__ == "__main__":
    f = sc.textFile("dataset/cars.data")

    # TODO: Parse data from file into an RDD
    clusters = get_clusters(f)

    for cluster in clusters:
        print(','.join(cluster))
