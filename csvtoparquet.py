import panda as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import sys

csv = sys.argv[1]

df = pd.dataframe(data=csv)

table = pa.Table.grom_pandas(df)

pq.write_table(table, 'movies_genre.parquet')
