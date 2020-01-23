
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

df_km.show(3)
print("mark1")
from pyspark.ml.clustering import KMeans
kmeans = KMeans(k=4, seed=1)
km_model = kmeans.fit(df_km)
print("mark2")
centers = km_model.clusterCenters()
print("mark3")
transformed = km_model.transform(df_km).select('state', 'prediction')

transformed.show(49)
file="/home/jzhu/desktop/Bigdata/Code/projetBigData/1_treat/result.csv"
#transformed.to_csv("%s.csv" % file, header=True)

