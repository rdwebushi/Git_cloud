from pyspark.sql import SparkSession
spark= SparkSession.builder.appName('Basic').getOrCreate()
df=spark.read.json('people.json')
#df.show()
#df.printSchema() #describes the dataframes
#print(df.describe())
#df.describe().show() #shows more data to it

print(df.columns)
print(df.describe())
