import pyspark
from pyspark.sql import SparkSession
import time

t1 = time.time()

spark = SparkSession.builder.appName("query2-sparksql-parquet").getOrCreate()

ratings = spark.read.parquet("hdfs://master:9000/ratings.parquet")

ratings.registerTempTable("ratings")

sqlString = \
        "select "+ \
	"((select count(distinct user) from ratings where user in "+ \
	"(select user from ratings group by user having avg(rating) > 3.0)) "+ \
	"/ (select count(distinct user) from ratings)) * 100 as average"

res = spark.sql(sqlString)

res.show()

res.write.format("csv").save("hdfs://master:9000/q2parquet")

t2 = time.time()

print(t2-t1)
