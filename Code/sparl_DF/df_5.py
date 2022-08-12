
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,IntegerType,StringType,StructType
spark= SparkSession.builder.appName('Basic').getOrCreate()

df=spark.read.csv('appl_stock.csv', inferSchema=True,header=True)

#collect() will  store the  result into a python List
filtered= df.filter(df['low']==197.16).select(['Open','close']).collect()
print(filtered)