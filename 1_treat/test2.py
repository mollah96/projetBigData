
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession

spark=SparkSession \
        .builder \
        .appName('aaaaaa') \
        .getOrCreate()


import pandas as pd
import numpy as np
from pyspark.sql import SQLContext


try:
    sc.stop()
except:
    pass
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext.getOrCreate(conf=conf)
sqlContext = SQLContext(sc)

from pyspark.ml.feature import PCA as PCAml
from pyspark.ml.linalg import Vectors 



df = pd.read_csv("state.csv")
sdf = sqlContext.createDataFrame(df)

sdf.show(3)


from pyspark.ml.feature import VectorAssembler
vecAss = VectorAssembler(inputCols = sdf.columns[1:3], outputCol = "features")
df_km = vecAss.transform(sdf).select("state","features")

df_km.show(3)



import plotly.express as px
fig = px.scatter(x=df.cost_per_student, y=df.event_count)
fig.show()