from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = (SparkSession
        .builder
        .appName('AutheorsAge')
        .getOrCreate())

data_df = spark.createDataFrame([("Sachine",49),("Rahul",43),("MSD",34),("Sunil",39),("Sachine",32),("Rahul",51),("MSD",54),("Sunil",63)],["name","age"])

avg_df =data_df.groupBy("name").agg (avg("age"))
avg_df.show()
