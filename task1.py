import time, sys, json
from pyspark.sql import SparkSession
import binascii,csv

st = time.time()

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

input_1 = sys.argv[1]
input_2 = sys.argv[2]
output = sys.argv[3]


def hash_it_2(x):
    a = hash(str(x))
    return a


def hash_it(x):
    p = 1710711741
    m = 10000
    list_a = [6383777788, 8189580011, 2538693988, 7425076232, 5097867820, 8009593258, 2438559140, 4453214645,
              3320639863,
              2883987868, 4445666501, 5649451162, 9354922952, 5399185838, 4464886692, 6863919033, 2910308964,
              6483746980,
              6553413196, 4296039188, 3449571386, 1549471493, 5554575630, 3383574969, 7201240094, 6666524841,
              4528372629,
              7078589071, 2978723210, 1642046119, 6811027205, 3764947109, 5490460361, 5167614792, 4285276616,
              5286033057,
              6281910103, 7127908639, 4135746128, 5431773516, 2433342381, 3509990093, 9186035294, 5016093060,
              2120171700,
              6039993304, 7991237996, 6954800230, 1903451028, 6476001290, 6562268092, 7772135538, 2362549642,
              6988865575,
              6386991515]

    list_b = [9086320966, 1831247637, 4084289813, 4317353021, 6423257930, 8101120749, 5897679258, 1850381232,
              8279075384,
              7385899354, 1722054685, 8676404255, 5157987520, 2893271444, 4777630142, 7861048072, 8058938804,
              1877393135,
              7217738322, 5871113862, 8430876679, 8894647165, 7684086563, 6698077648, 4673475296, 2999318486,
              7458608345,
              7647347112, 2481713990, 8238141148, 3980085040, 6920973187, 5700952884, 6944873753, 6966214079,
              8078473969,
              7177631464, 6063822529, 8126688502, 4817575610, 6299201932, 7716234183, 2595374108, 5515334407,
              4490062818,
              3170592174, 7978764573, 3117344817, 6678338214, 6009207581, 8802448280, 1768125729, 7233984239,
              2104506893,
              6930484268]
    index = x
    temp = []
    for ii in range(7):
        a = list_a[ii]
        b = list_b[ii]
        ee = ((a * index + b) % p) % m
        temp.append(ee)
    return hash_it_2(temp)





def get_result(x, y):
    if x is None or x == "":
        return [0]
    else:
        xx = int(binascii.hexlify(x.encode('utf8')), 16)
        hash_value = {hash_it(xx)}
        if hash_value.issubset(set(y)):
            return [1]
        else:
            return [0]


business_1 = sc.textFile(input_1).map(lambda x: json.loads(x))
city_of_one = business_1.map(lambda x: x["city"]).distinct().filter(lambda x: x != "").map(
    lambda x: int(binascii.hexlify(x.encode('utf8')), 16)).map(lambda x: hash_it(x)).collect()


business_2 = sc.textFile(input_2).map(lambda x: json.loads(x))
city_of_two = business_2.map(lambda x: x["city"]).flatMap(lambda x: get_result(x, city_of_one))

this_is = city_of_two.collect()

with open(output, "w+", newline="") as output_file:
    writer = csv.writer(output_file, delimiter=' ')
    writer.writerow(this_is)

print(time.time() - st)
