import csv
from pyspark.sql import SparkSession
import time

t1 = time.time()

spark = SparkSession.builder.appName("query3-sparksql-csv").getOrCreate()

ratings = spark.read.format('csv').options(header='false', inferSchema='true').load("hdfs://master:9000/ratings.csv")
genres = spark.read.format('csv').options(header='false', inferSchema='true').load("hdfs://master:9000/movie_genres.csv")

ratings.registerTempTable("ratings")
genres.registerTempTable("genres")

sqlTempString = \
	"select r._c1 as id, avg(r._c2) as temp_avg, g._c1 as genre "+ \
	"from ratings as r inner join genres as g on r._c1 = g._c0 "+ \
	"group by r._c1, g._c1"

tempRes = spark.sql(sqlTempString)

tempRes.registerTempTable("tempRes")

sqlString = \
	"select distinct g._c1 as genre, (select avg(t.temp_avg) from tempRes as t where t.genre = g._c1) as average_rating, (select count(distinct r1._c1) from ratings as r1 inner join genres as g1 on r1._c1 = g1._c0 where g1._c1 = g._c1) as number "+ \
	"from genres as g"

res = spark.sql(sqlString)

res.show()

res.write.format("csv").save("hdfs://master:9000/q3csv")

t2 = time.time()

print(t2-t1)
