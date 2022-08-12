from pyspark.sql import SparkSession 
spark = SparkSession.builder.appName('Basics').getOrCreate()

df= spark.read.csv('appl_stock.csv', inferSchema =True, header=True)
#df.filter((df ['close']<500) &(df ['Open']>200)).select(['Open','close']).show()

#spark always show 20 rows
#to show other rows
df.filter((df ['close']<500) &(df ['Open']>200)).select(['Open','close']).show(1200)
