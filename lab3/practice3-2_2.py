from pyspark import SparkContext, SparkConf

# Step 讀取資料(RDD格式)
master = "yarn"
conf = SparkConf().setMaster(master)
sc = SparkContext(conf=conf)
lines = sc.textFile("hdfs://master:9000/user/s111526005/lab3/practice4/practice3-2_2.txt")
data = lines.map(lambda line:line.split(","))

# 定義距離公式
def get_distance(data):
    x1 = float(data[0][0])
    x2 = float(data[1][0])
    y1 = float(data[0][1])
    y2 = float(data[1][1])
    result = float(((x2 - x1)**2 + (y2 - y1)**2) ** 0.5)
    return result

# 用笛卡爾將資料做內積運算
carts = data.cartesian(data)
# 呼叫距離公式對兩點做計算
distances = carts.map(lambda cart:get_distance(cart))
# 將等於0的結果去除
distances_without_zero = distances.filter(lambda distance:distance!=0.0)
# 取得最小值
result = distances_without_zero.min()

print(result)