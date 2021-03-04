from io import StringIO
import csv
from pyspark import SparkConf, SparkContext

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=","))[0]

def f1(line):
    year = str(line[1][0][1])
    
    if year[2]=='0':
        if int(year[3])<5: return ( '2000-2004', ( line[1][0][0],1 ) )
        else: return ( '2005-2009', ( line[1][0][0], 1 ) )
    else:
        if int(year[3])<5: return ( '2010-2014', ( line[1][0][0], 1 ) )
        else: return ( '2015-2019', ( line[1][0][0], 1 ) ) 

def f2(x,y):
    return (x[0]+y[0],x[1]+y[1])

def f3(line):
    return (line[0],(line[1][0]/line[1][1],line[1][1]))

if __name__=='__main__':
    conf = SparkConf().setAppName("Q4")
    sc = SparkContext(conf=conf)


#---------------------------------------------------------------------------#
#           |      (0)                      (1)                      |      #
#  Movies:  | ( id_movie, ( length of description, year ) )          |      #
#           |                                                        |      #
#---------------------------------------------------------------------------#

#   After map-filter-map, movies must look like above    

    movies = sc.textFile("hdfs://master:9000/movies.csv")\
               .map(lambda line: split_complex(line))\
               .filter(lambda line: line[2]!='' and line[3]!='')\
               .filter(lambda line: line[2]!='' and int(line[3].split('-')[0])>=2000)\
               .map(lambda line: ( line[0], ( len(line[2].split(" ")), int(line[3].split('-')[0]) )))        

#-----------------------------------------------#
#           |      (0)      (1)    |            #
#  Genres:  | ( id_movie,  genre ) |            #
#           |                      |            #
#-----------------------------------------------#

#   After map-filter-map, genres must look like above  

    genres = sc.textFile("hdfs://master:9000/movie_genres.csv")\
               .map(lambda line: split_complex(line))\
               .filter(lambda line: line[1]=='Drama')\
               .map(lambda line: ( line[0], line[1] ))


#------------------------------------------------------------------------#
#           |      (0)                     (1)                 |         #
#  Data:    | ( id_movie, ( ( length, year ), ( genre ) )      |         #
#           |                                                  |         #
#------------------------------------------------------------------------#                            

#   Join movies and genres with id_movie as key    
    
    data = movies.join(genres)
    data = data.map(f1).reduceByKey(f2)
    data = data.map(f3)	

    data.saveAsTextFile("hdfs://master:9000/res4")

    
