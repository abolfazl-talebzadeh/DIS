from pyspark.sql import SparkSession
from pyspark import SparkContext
from time import time
import csv
import re

appName = "MyApp"

# spark = SparkSession.builder \
#     .appName(appName) \
#     .getOrCreate()

sc = SparkContext()

dataRdd = sc.textFile("hdfs://namenode:9000/dis_materials/1.tsv")
#data = spark.read.csv("hdfs://namenode:9000/dis_materials/1.tsv",header = 'true', sep = "\t")
def mapper(line):
    r = {}
    counter = 0
    score = 0
    resourceList = {}
    wordDic = {}
    with open("/home/ubuntu/AFINN-111.txt", 'r') as f:
        data = csv.reader(f,delimiter='\t')
        resourceList= dict(data)
    for l in resourceList.keys():
        r[l]=float(resourceList[l])
    lineList=line.split("\t")
    if isinstance(lineList[0],str)!=True or isinstance(lineList[1],str)!=True:
        return "header",(0,0)
    date = lineList[14][0:7]
    cleanText = re.sub(r"[0-9(-_:;,.!?@#$%^&*)']",'',lineList[13].lower())
    textList = cleanText.split(" ")
    for i in textList:
        if i in r.keys():
            if i in wordDic.keys():
                wordDic[i]+=r[i]
                counter+=1
                score +=r[i]
            else:
                counter+=1
                wordDic[i]=r[i]
                score+=r[i]
    if score != 0:
        out=score/counter
    else:
        out = score
    return  date, (out,1)


def m(x):
    if x[1][1]<1:
        return x[0],0
    return x[0],x[1][0]/x[1][1] 

#a= data.take(5)
#print(a)
a = dataRdd.map(mapper).reduceByKey(lambda e1,e2: (e1[0]+e2[0],e1[1]+e2[1])).map(m).collect()

#print(f"Elapsed time = {int(elapsedTime//60)}:{int(elapsedTime%60)}")

