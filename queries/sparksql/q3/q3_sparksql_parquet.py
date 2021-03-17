import pyspark
from pyspark.sql import SparkSession
import time

t1 = time.time()

spark = SparkSession.builder.appName("query2-sparksql-parquet").getOrCreate()

ratings = spark.read.parquet("hdfs://master:9000/ratings.parquet")
genres = spark.read.parquet("hdfs://master:9000/movie_genres.parquet")

ratings.registerTempTable("ratings")
genres.registerTempTable("genres")

sqlTempString = \
	"select r.id_movie as id, avg(r.rating) as temp_avg, g.genre as genre "+ \
	"from ratings as r inner join genres as g on r.id_movie = g.id_movie "+ \
	"group by r.id_movie, g.genre"

tempRes = spark.sql(sqlTempString)

tempRes.registerTempTable("tempRes")

sqlString = \
	"select distinct g.genre as genre, (select avg(t.temp_avg) from tempRes as t where t.genre = g.genre) as average_rating, (select count(distinct r1.id_movie) from ratings as r1 inner join genres as g1 on r1.id_movie = g1.id_movie where g1.genre = g.genre) as number "+ \
	"from genres as g"

res = spark.sql(sqlString)

res.show()

res.write.format("csv").save("hdfs://master:9000/q3parquet")

t2 = time.time()

print(t2-t1)
