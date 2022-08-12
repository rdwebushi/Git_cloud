from ctypes import Structure
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,IntegerType,StringType,StructType
spark= SparkSession.builder.appName('Basic').getOrCreate()

df=spark.read.csv('appl_stock.csv', inferSchema=True,header=True)
#df.printSchema() #to show schema

df.filter("close < 500").show()     #to display data of close less than 500, in this all column will display
df.filter('close>500').select(['high','Open','close']).show()
# specific filter foe data tobe less than 500