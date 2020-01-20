from pyspark import SparkContext
from pyspark.sql import SparkSession

spark=SparkSession \
        .builder \
        .appName('my_first_app_name') \
        .getOrCreate()

import pandas as pd
import numpy as np

sc = SparkContext( 'local', 'test')
logFile = "file:////home/jzhu/desktop/Bigdata/TP_Hadoop/LoremIpsum-128"
logData = sc.textFile(logFile, 2).cache()
numAs = logData.filter(lambda line: 'a' in line).count()
numBs = logData.filter(lambda line: 'b' in line).count()
print('Lines with a: %s, Lines with b: %s' % (numAs, numBs))
monthlySales = spark.read.csv(file, header=True, inferSchema=True，sep='/t')
monthlySales.show()