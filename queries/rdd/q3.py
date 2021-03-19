from io import StringIO
import csv
from pyspark import SparkConf, SparkContext

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=","))[0]

def f1(x,y):
    return (x[0]+y[0],x[1]+y[1])

def f2(x,y):
    return x/y

def f3(line):
    return (line[1][0],(line[1][1][0]/line[1][1][1],1))	

if __name__ == "__main__":
    conf = SparkConf().setAppName("Q2")
    sc = SparkContext(conf=conf)

#############################################################################################################
#	|    |0|   | 	       |1|	    |
#	| id_movie | (sum_rating, sum_count)|                 CHECK!!
#       |          |                        |
    ratings = sc.textFile("hdfs://master:9000/ratings.csv")\
                .map(lambda line: split_complex(line))\
                .filter(lambda line: float(line[2])!=None)\
                .map(lambda line: (line[1],(float(line[2]),1)))\
                .reduceByKey(f1)\
                .reduceByKey(f2)

#############################################################################################################    
#      |      0       1   | 
#      |( id_movie, genre)|				      CHECK!!
#      |                  |
    genres = sc.textFile("hdfs://master:9000/movie_genres.csv")\
               .map(lambda line: split_complex(line))\
               .map(lambda line: (line[0],line[1])).distinct()
    
#############################################################################################################
#      |      0                    1                     |
#      |( id_movie, ((g1,..,gN),(sum_rating,sum_count)) )|    CHECK!!
#      |                                                 |
    data = genres.join(ratings)
    data = data.map(f3).reduceByKey(f1)
    data = data.map(lambda line: (line[0],(line[1][0]/line[1][1],line[1][1])))

    data.saveAsTextFile("hdfs://master:9000/res3")
