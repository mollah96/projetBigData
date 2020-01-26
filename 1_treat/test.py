
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
vecAss = VectorAssembler(inputCols = sdf.columns[1:], outputCol = "features")
df_km = vecAss.transform(sdf).select("state","features")

'''
cost = list(range(2,20))
for k in range(2, 20):
    kmeans = KMeans(k=k, seed=1)
    km_model = kmeans.fit(df_km)
    # computeCost
    cost[k-2] = km_model.computeCost(df_km)
'''

df_km.show(3)
from pyspark.ml.clustering import KMeans
kmeans = KMeans(k=4, seed=1)
km_model = kmeans.fit(df_km)
centers = km_model.clusterCenters()
transformed = km_model.transform(df_km).select('state', 'prediction')

transformed.show(49)

