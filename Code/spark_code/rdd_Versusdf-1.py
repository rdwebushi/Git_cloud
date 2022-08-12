from pyspark import SparkContext 
sc = SparkContext()
dataRDD = sc.parallelize([("Sachine",49),("Rahul",43),("MSD",34),("Sunil",39),("Sachine",32),("Rahul",51),("MSD",54),("Sunil",63)]) #parallalizing data
agesRDD = (dataRDD.map(lambda x:(x[0],(x[1],1)))        #mapping
.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))          #shuffling
.map(lambda x:(x[0],x[1][0]/x[1][1])))                  #calculate average
print(agesRDD.collect())                                #actions

#from pyspark import SparkContext 
#sc = SparkContext()
#dataRDD = sc.parallelize([("Sachine",49),("Rahul",43),("MSD",34),("Sunil",39),("Sachine",32),("Rahul",51),("MSD",54),("Sunil",63)]) #parallalizing data
#agesRDD1 = (dataRDD.map(lambda x:(x[0],(x[1],1)))        #mapping
#agesRDD2 = (dataRDD1.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])))          #shuffling
#agesRDD3 =(dataRDD2.map(lambda x:(x[0],x[1][0]/x[1][1])))                  #calculate average
#print(agesRDD3.collect())                                #actions
