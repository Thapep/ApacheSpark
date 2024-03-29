import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import sys

csv1 = sys.argv[1]
csv2 = sys.argv[2]
csv3 = sys.argv[3]

movie_cl = ['id_movie', 'name', 'description', 'date', 'duration', 'cost', 'income', 'popularity']
genre_cl = ['id_movie','genre']
ratings_cl = ['user','id_movie','rating','timestamp']

df1 = pd.read_csv(csv1, names=movie_cl,header=None )
df2 = pd.read_csv(csv2, names=genre_cl, header=None )
df3 = pd.read_csv(csv3, names=ratings_cl, header=None )

df1.to_parquet('movies.parquet')
df2.to_parquet('movie_genres.parquet')
df3.to_parquet('ratings.parquet')
