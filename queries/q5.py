from io import StringIO
import csv
from pyspark import SparkConf, SparkContext

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=","))[0]

def f1(line):
    return ((line[1][0][1],line[1][1][0]), (line[1][0][0][0],line[1][1][1],line[1][0][0][1]) ) 

def f2(x,y):
    return x+y

def f3(line):
    return (line[0][0], (line[0][1],line[1],int(len(line[1])/3) ))

def f4(x,y):

    if x[2]>=y[2]:
        return x
    else:
        return y 

def f5(line):
    dct = {line[1][1][i+1]: (line[1][1][i],line[1][1][i+2]) for i in range(0, len(line[1][1]), 3)}
    M = (dct[max(dct)][0],dct[max(dct)][1],max(dct))
    m = (dct[min(dct)][0],dct[min(dct)][1],min(dct))
    
    dM = {}
    dm = {}
    for i in range(0, len(line[1][1]), 3):
        if M[2]==line[1][1][i+1]:
            dM[line[1][1][i+2]] = (line[1][1][i],line[1][1][i+1])

        if m[2]==line[1][1][i+1]:
            dm[line[1][1][i+2]] = (line[1][1][i],line[1][1][i+1])

    
    M = dM[max(dM)]
    m = dm[max(dm)]

    return ( line[0], (line[1][0], line[1][2], M+m)) 

if __name__=='__main__':
    conf = SparkConf().setAppName("Q5")
    sc = SparkContext(conf=conf)

    title = sc.textFile("hdfs://master:9000/movies.csv")\
               .map(lambda line: split_complex(line))\
               .map(lambda line: (line[0], (line[1], float(line[7]))))
# title: ( id_movie, (title, popularity) )

    genres = sc.textFile("hdfs://master:9000/movie_genres.csv")\
               .map(lambda line: split_complex(line))\
               .map(lambda line: (line[0], line[1]))
# genres: ( id_movie, genre )

    movies = title.join(genres)
# movies: ( id_movie, (title, popularity), genre )

    ratings = sc.textFile("hdfs://master:9000/ratings.csv")\
               .map(lambda line: split_complex(line))\
               .map(lambda line: (line[1], (line[0],line[2])))
# ratings: ( id_movie, (user, rating) )

#---------------------------------------------------------------------------------------#
#           |      (0)                      (1)                      		 |      #
#  Data:    | ( id_movie, ( ((title, popularity) , genre), (user, rating) ) )    |      #
#           |                                                        		 |      #
#---------------------------------------------------------------------------------------#
    data = movies.join(ratings)

    data = data.map(f1).reduceByKey(f2)                        
# after map:    ( (genre,user), (title,rating,popularity) )
# after reduce: ( (genre,user), list(title,rating,popularity) )                 

    data = data.map(f3).reduceByKey(f4)
# after map:    ( genre, (user,(list of title,popularity,rating), sum_count ) )
# after reduce: keeps the user with most ratings by genre 
    data = data.map(f5).sortByKey()

    data.saveAsTextFile("hdfs://master:9000/res5")
