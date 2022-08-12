#combination of group by with agregation 
from audioop import avg
from itertools import count
from pyspark.sql import SparkSession

#for the other equivalent syntax we need to import "functions" "countDistinct"(avg, sum,min,max,count)
from pyspark.sql.functions import countDistinct, avg, sum, min, max, count

spark= SparkSession.builder.appName('Basics').getOrCreate()

df=spark.read.csv("sales_info-2.csv", inferSchema=True, header=True)

#combination of groupby
group_data= df.groupBy('company')

group_data.agg({'sales':'sum'}).show()
#group_data.agg({'sales':'min'}).show()
#group_data.agg({'sales':'max'}).show()

#another syntax for same functionality

df.select(avg ('Sales')).show()

