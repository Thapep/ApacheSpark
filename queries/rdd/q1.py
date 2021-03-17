#import pandas as pd
#import numpy as np
#import pyarrow as pa
#import pyarrow.parquet as pq
#import sys
from io import StringIO
import csv
from pyspark import SparkConf, SparkContext

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=","))[0]

def max(n,p):

    if n[1]>=p[1]:
        return n
    else:
        return p 

if __name__ == "__main__":
    conf = SparkConf().setAppName("Q1")
    sc = SparkContext(conf=conf)

    data = sc.textFile("hdfs://master:9000/movies.csv")

    # split collumn with commas 
    data = data.map(lambda line: split_complex(line))
    
    # discard lines with date<2000 or with no date value or 0 income/cost and cache the dataset for better performance
    data = data.filter(lambda line: line[3]!='' and int(line[5])!=0 and int(line[6])!=0).cache()
    data = data.filter(lambda line: int(line[3].split("-")[0])>=2000).cache()

    #                           key: year          value: (name, profit)             
    data = data.map(lambda line: (int(line[3].split("-")[0]), (line[1], ((int(line[6])-int(line[5]))/int(line[5]))*100)))
    data = data.reduceByKey(max)
    
    data.collect()	
    data = sc.parallelize(data)
    data.saveAsTextFile("hdfs://master:9000/res2")
    

    


