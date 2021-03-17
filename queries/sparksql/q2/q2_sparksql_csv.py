import csv
from pyspark.sql import SparkSession
import time

t1 = time.time()

spark = SparkSession.builder.appName("query2-sparksql-csv").getOrCreate()

ratings = spark.read.format('csv').options(header='false', inferSchema='true').load("hdfs://master:9000/ratings.csv")

ratings.registerTempTable("ratings")

sqlString = \
	"select "+ \
	"((select count(distinct _c0) from ratings where _c0 in "+ \
	"(select _c0 from ratings group by _c0 having avg(_c2) > 3.0)) "+ \
	"/ (select count(distinct _c0) from ratings)) * 100 as average"

res = spark.sql(sqlString)

res.show()

res.write.format("csv").save("hdfs://master:9000/q2csv")

t2 = time.time()

print(t2-t1)
