from ctypes import Structure
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,IntegerType,StringType,StructType
spark= SparkSession.builder.appName('Basic').getOrCreate()

#third parameter indicates null allowded?

data_schema = [StructField('age',IntegerType(),True),
                StructField('name',StringType(),True)]

final_struct=StructType(fields=data_schema)

df=spark.read.json('people.json', schema=final_struct)
#df.show()
#df.printSchema() #describes the dataframes
print(df.describe())
df.describe().show()
print(df.columns)
#print()
