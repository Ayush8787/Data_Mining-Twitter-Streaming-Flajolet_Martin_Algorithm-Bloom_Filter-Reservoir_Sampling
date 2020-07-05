import time, sys, json
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
import binascii, csv

st = time.time()

port = sys.argv[1]
out = sys.argv[2]

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sc, 5)
input_streaming = ssc.socketTextStream("localhost", int(port))

sc.setLogLevel("ERROR")

f = open(out, "w")
f.write("Time,Ground Truth,Estimation")
f.close()


def hash_it(x, freq):

    list_a = [1, 11, 17, 37, 3, 11, 23, 39, 5, 17, 29, 43, 1, 11, 17, 39, 3, 13, 21, 39, 5, 17, 31, 47, 1, 13, 17, 39, 3, 11, 23, 43, 5, 17, 29, 41, 5, 17, 31, 43]

    list_b = [15, 47, 99, 196, 25, 63, 119, 223, 36, 81, 146, 252, 25, 64, 121, 221, 36, 83, 142, 254, 16, 47, 99,
              197, 36, 81, 139, 256, 17, 47, 100, 185, 25, 64, 121, 224, 27, 65, 123, 231]

    list_m = [191, 1541, 12187, 195511, 381, 3071, 24691, 393139, 761, 5991, 92315, 776233, 768, 6111, 97919, 776998,
              193, 1543, 12279, 196599, 389, 3078,
              24589, 393239, 387, 3089, 24589, 393243, 767, 6149, 97917, 786431, 193, 1543, 11999, 196611, 199, 1645,
              122991, 199671]

    index = int(binascii.hexlify(x.encode('utf8')), 16)
    temp = []
    for ii in range(40):
        a = list_a[ii]
        b = list_b[ii]
        p = list_m[ii]
        ee = ((a * index + b) % p) % 200
        binary_value = bin(ee)[2:]
        aa = len(binary_value)
        bb = len(binary_value.rstrip('0'))
        zero = aa - bb
        ans = max(freq[ii], zero)
        freq[ii] = ans
    return 1


def get_result(time, x):
    global out, f
    rdd_iterate = x.collect()
    ground_truth = len(list(set(rdd_iterate)))

    hashed_cities = []
    frequent = [0] * 40
    for i in rdd_iterate:
        hash_it(i, frequent)

    temp_1 = []
    for i in frequent:
        temp_1.append(2 ** i)

    ll1 = [(0, 10), (10, 20), (20, 30), (30, 40)]
    avg = []
    for i in ll1:
        temp = temp_1[i[0]:i[1]]
        sum1 = sum(temp)
        avg1 = sum1/10
        avg.append(avg1)
    avg.sort()
    median = avg[2]

    f = open(out, "a")
    f.write(f"\n{time},{ground_truth},{median}")
    f.close()


data_stream = input_streaming.window(30, 10).map(lambda row: json.loads(row)).map(lambda kv: kv["city"]) \
    .filter(lambda city_str: city_str != "").foreachRDD(get_result)
ssc.start()
ssc.awaitTermination()
