from pyspark.sql import SparkSession
import time

t1 = time.time()

spark = SparkSession.builder.appName("query5-sparksql-parquet").getOrCreate()

movies = spark.read.parquet("hdfs://master:9000/movies.parquet")
ratings = spark.read.parquet("hdfs://master:9000/ratings.parquet")
genres = spark.read.parquet("hdfs://master:9000/movie_genres.parquet")

movies.registerTempTable("movies")
ratings.registerTempTable("ratings")
genres.registerTempTable("genres")

#gives each group of users and genres a serial number in descending movies number order, this way each genre's highest number of ratings from a single user has serial number 1
sqlTempString = \
        "select g.genre as genre, r.user as user_id, count(*) as user_ratings, row_number() over (partition by g.genre order by count(*) desc) as aa "+ \
        "from ratings as r inner join genres as g on r.id_movie = g.id_movie "+ \
        "group by g.genre, r.user"

tempRes = spark.sql(sqlTempString)

tempRes.registerTempTable("tempRes")

#gives each movie of a single genre and a single user a serial number in descending rating order and descending popularity order,
#this way the highest rated(and most popular if two movies have the same rating)
#movie of a genre from the specific user has serial number 1
sqlTempString4 = \
        "select g.genre as genre, r.user as user_id, m.name as movie_name, row_number() over (partition by r.user, g.genre order by r.rating desc, m.popularity desc) as aa "+ \
        "from ratings as r inner join genres as g on r.id_movie = g.id_movie "+ \
        "inner join movies as m on r.id_movie = m.id_movie"

tempRes4 = spark.sql(sqlTempString4)

tempRes4.registerTempTable("tempRes4")

#gives each movie of a single genre and a single user a serial number in ascending rating order and descending popularity order,
#this way the lowest rated(and most popular if two movies have the same rating)
#movie of a genre from the specific user has serial number 1
sqlTempString5 = \
        "select g.genre as genre, r.user as user_id, m.name as movie_name, row_number() over (partition by r.user, g.genre order by r.rating, m.popularity desc) as aa "+ \
        "from ratings as r inner join genres as g on r.id_movie = g.id_movie "+ \
        "inner join movies as m on r.id_movie = m.id_movie"

tempRes5 = spark.sql(sqlTempString5)

tempRes5.registerTempTable("tempRes5")

sqlTempString2 = \
        "select t.genre as genre, t.user_id as user_id, t.user_ratings as user_ratings, "+ \
        "t4.movie_name as max_rating_movie, "+ \
        "(select max(r.rating) from ratings as r inner join genres as g on r.id_movie = g.id_movie where g.genre = t.genre and r.user = t.user_id) as max_rating, "+ \
        "t5.movie_name as min_rating_movie, "+ \
        "(select min(r.rating) from ratings as r inner join genres as g on r.id_movie = g.id_movie where g.genre = t.genre and r.user = t.user_id) as min_rating "+ \
        "from tempRes as t inner join tempRes4 as t4 on t.genre = t4.genre and t.user_id = t4.user_id "+ \
        "inner join tempRes5 as t5 on t.genre = t5.genre and t.user_id = t5.user_id "+ \
        "where t.aa = 1 and t4.aa = 1 and t5.aa = 1 "+ \
	"order by t.genre"

res = spark.sql(sqlTempString2)

res.show()

res.write.format("csv").save("hdfs://master:9000/q5parquet")

t2 = time.time()

print(t2-t1)
