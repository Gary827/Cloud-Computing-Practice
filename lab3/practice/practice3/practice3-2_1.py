from pyspark import SparkContext, SparkConf

master = "yarn"

conf = SparkConf().setMaster(master)
sc = SparkContext(conf=conf)

#Change your HDFS Server Name & Account first
lines = sc.textFile("hdfs://master:9000/user/s111526005/lab3/practice3/practice3-2_1.txt")

filter_result=lines.filter(lambda line: "shoe" in line)
print ("filter_result:",filter_result.collect())