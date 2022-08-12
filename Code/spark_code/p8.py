import sys
from pyspark import SparkConf, SparkContext 
sc=SparkContext("local","Student example")
studentMarkData= sc.textFile("student.txt")
studentLines= studentMarkData.map(lambda l:l.split(","))

#first year marks and filter
firstYearMarks= studentLines.filter(lambda x: x[1]=='year1')
firstYearMarksMean=firstYearMarks.map(lambda x: [x[0],x[1],(float(x[2])+float(x[3]))/2])
firstYearMarksMeanList=firstYearMarksMean.collect()

#second year mark and list
secondYearMarks=studentLines.filter(lambda x: x[1]=='year2')
secondYearMarksMean=secondYearMarks.map(lambda x: [x[0],x[1],(float(x[2])+float(x[3]))/2])

secondYearMarksMeanList=secondYearMarksMean.collect()
print(secondYearMarksMeanList)
i=0

print("list of students whos second year agrigate is higher than first year")

for firstListElement in firstYearMarksMeanList:
    secondListElement = secondYearMarksMeanList[i]

    if (secondListElement[2] > firstListElement[2]):
        print("student : %s, second year aggrigate: %f, first year Aggregate: %f "\
            %(firstListElement[0],secondListElement[2],firstListElement[2]))
    i +=1
