import pyspark
from pyspark.sql import SparkSession
import time

t1 = time.time()

spark = SparkSession.builder.appName("query1-sparksql-parquet").getOrCreate()

movies = spark.read.parquet("hdfs://master:9000/movies.parquet")

movies.createOrReplaceTempView("movies")

sqlString = "select year(date) as year, name as title,((income-cost)/cost)*100 as profit from movies where ((income-cost)/cost)*100 in (select max(((income-cost)/cost)*100) "+ \
            "from movies "+ \
            "where year(date)>=2000 and income<>0 and cost<>0 and date is not null group by year(date)) "+ \
            "order by year"

res = spark.sql(sqlString)

res.show()

res.write.format("csv").save("hdfs://master:9000/q1parquet")

t2 = time.time()

print(t2-t1)
