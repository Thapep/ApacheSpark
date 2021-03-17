import csv
from pyspark.sql import SparkSession
import time

t1 = time.time()

spark = SparkSession.builder.appName("query1-sparksql-csv").getOrCreate()

movies = spark.read.format('csv').options(header='false', inferSchema='true').load("hdfs://master:9000/movies.csv")

movies.registerTempTable("movies")

sqlString = "select year(_c3) as year, _c1 as title, ((_c6-_c5)/_c5)*100 as profit from movies where ((_c6-_c5)/_c5)*100 in (select max(((_c6-_c5)/_c5)*100) "+ \
            "from movies "+ \
            "where year(_c3)>=2000 and _c6<>0 and _c5<>0 and _c3 is not null group by year(_c3)) "+ \
            "order by year"

res = spark.sql(sqlString)

res.show()

res.write.format("csv").save("hdfs://master:9000/q1csv")

t2 = time.time()

print(t2-t1)
