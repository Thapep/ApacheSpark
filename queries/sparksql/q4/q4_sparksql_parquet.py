import re
from pyspark.sql import SparkSession
import time

t1 = time.time()

#counts the number of words of a string, filters out special symbols and double spaces
def avrg_count(x):
        num_words = len(re.sub(r'[^a-zA-Z0-9 ]', '', str(x)).split())
        return num_words

spark = SparkSession.builder.appName("query4-sparksql-parquet").getOrCreate()

movies = spark.read.parquet("hdfs://master:9000/movies.parquet")
genres = spark.read.parquet("hdfs://master:9000/movie_genres.parquet")

movies.registerTempTable("movies")
genres.registerTempTable("genres")
spark.udf.register("avg_count", avrg_count)

sqlTempString = \
        "select m.name as title, (avg_count(m.description)) as average_count, year(m.date) as year "+ \
        "from movies as m inner join genres as g on m.id_movie = g.id_movie "+ \
        "where g.genre = 'Drama' and year(m.date)>=2000 and m.description<>'' and m.description is not null"

tempRes = spark.sql(sqlTempString)

tempRes.registerTempTable("tempRes")

sqlString = \
        "select concat(5*floor(year/5), '-', 5*floor(year/5)+4) as years, avg(average_count) as average_description_length "+ \
        "from tempRes "+ \
        "group by 1 "+ \
        "order by 1"

res = spark.sql(sqlString)

res.show()

res.write.format("csv").save("hdfs://master:9000/q4parquet")

t2 = time.time()

print(t2-t1)
