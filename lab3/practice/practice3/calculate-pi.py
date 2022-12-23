# Linking Spark & set Spark Manager
from pyspark import SparkContext, SparkConf
import random

appName = "calculate-pi"
master = "yarn"

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

# Pi Estimation
def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

NUM_SAMPLES=10**6
count = sc.parallelize(range(0, NUM_SAMPLES)).filter(inside).count()
print ("Pi is roughly %f" % (4.0 * count / NUM_SAMPLES))