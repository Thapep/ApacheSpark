from io import StringIO
import csv
from pyspark import SparkConf, SparkContext

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=","))[0]

def f1(x,y):
	
	return (x[0]+y[0],x[1]+y[1])

def f2(x,y):
	
	return (x/(x+y)*100)
	
if __name__ == "__main__":
    conf = SparkConf().setAppName("Q2")
    sc = SparkContext(conf=conf)

    data = sc.textFile("hdfs://master:9000/ratings.csv")
    data = data.map(lambda line: split_complex(line))

    data = data.map(lambda line: (line[0],(float(line[2]),1))).cache()
    data = data.reduceByKey(f1)

    data = data.map(lambda line: (2,1) if line[1][0]/line[1][1]>3 else (1,1)).cache()
    data = data.reduceByKey(lambda x, y: x+y)

    data = data.map(lambda line: (1,line[1])).cache()
    data = data.reduceByKey(f2)

    data.saveAsTextFile("hdfs://master:9000/res2")
