# program to filter the data, upgrading and managing data
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('Basic').getOrCreate()

df=spark.read.csv("dummy.csv",inferSchema=True,header=True)
df.show()

#Drop rows that have any null value columns
#df.na.drop().show()

#a row that must have at least two null value columns to appear in the row
#df.na.drop(thresh = 2).show()

#Drop rows that have a null value columns
df.na.drop(how='any').show()

#Drop rows that have all null value colums
df.na.drop(how = 'all').show()

#drop rows that have all null value in the sales column
df.na.drop(subset=['Bal']).show()

#fill null values with our values 
df.na.fill('DUMMY INSERT').show()

#fill null values with zeros for numaric columns
df.na.fill(0).show()
#fill null values only for name column with our own value
df.na.fill('No Name',subset=['Name']).show()
