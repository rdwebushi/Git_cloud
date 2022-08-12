"""find total average balance of blue=collar divorced people """
import sys
from pyspark import SparkConf,SparkContext
sc= SparkContext()
"""
div=sc.textFile('bank.csv')\
    .map(lambda x:(x.split(',')[2],x))\
    .filter(lambda x: x[0] =='divorced') \
    .reduceByKey(lambda x, y:x+y)\
    .collect()

blue=sc.textFile('bank.csv')\
    .map(lambda x:(x.split(',')[1],1))\
    .filter(lambda x: x[0] =='blue-collar') \
    .reduceByKey(lambda x, y:x+y)\
    .collect()    

print(div)
print(blue)
sc.stop()

#(lambda x:(x.split(',')[1],1))
#==(lambda x: x[0] =='blue-collar')"""
#select the file
bankData = sc.textFile("bank.csv")

banklines=bankData.map(lambda l:l.split(","))

#only get job, marrital status, and balance field

selectBankFields=banklines.map(lambda x: (x[1],x[2],x[5]))
filteredData=selectBankFields.filter(lambda f:(f[0]=='blue-collar' and f[1]=='divorced'))

#only get the balance field - convert to integer
onlyBalance=filteredData.map(lambda x:int(x[2]))
#convert to a list 
balanceList= onlyBalance.collect()
print(balanceList)
avgBal = sum (balanceList) / len (balanceList)
print("Average Balance is :%d"%avgBal)
