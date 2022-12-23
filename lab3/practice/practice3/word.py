from pyspark import SparkContext, SparkConf

master = "yarn"

conf = SparkConf().setMaster(master)
sc = SparkContext(conf=conf)

#Change your HDFS Server Name & Account first
lines = sc.textFile("hdfs://master:9000/user/s111526005/word.txt")

map_result=lines.map(lambda line: line.split(" "))
print ("map_result:",map_result.collect())

flatMap_result=lines.flatMap(lambda line: line.split(" "))
print ("flatMap_result:",flatMap_result.collect())

filter_result=lines.filter(lambda line: "Spark" in line)
print ("filter_result:",filter_result.collect())

rdd1=sc.parallelize([1,2,3,4])
print ("reduce_result:",rdd1.reduce(lambda a,b:a+b))

print ("count_result:",rdd1.count())

print ("collect_result:",rdd1.collect())