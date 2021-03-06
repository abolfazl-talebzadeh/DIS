
from time import time

df = spark.read.csv("hdfs://group2:9000/dis_materials/sample.tsv",header = 'true', sep = "\t")

def mapper(line):
    import csv
    import re
    import statistics
    r = {}
    counter = 0
    score = 0
    resourceList = []
    wordDic = {}
    with open("/home/ubuntu/AFINN-111.txt", 'r') as f:
        data = csv.reader(f,delimiter='\t')
        resourceList= list(data)
    for l in resourceList:
        r[l[0]]=float(l[1])
    lineList=line.split("\t")
    if isinstance(lineList[0],str)!=True or isinstance(lineList[1],str)!=True:
        return "header",(0,0)
    date = lineList[14][0:7]
    cleanText = ''.join(w.lower() for w in lineList[13] if w.isalpha() or w == " ")
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


startTime = time()

a = df.select("review_body","review_date")

b = a.rdd.map(mapper).reduceByKey(lambda e1,e2: (e1[0]+e2[0],e1[1]+e2[1]))

c = b.map(m)

d = c.toDF()

elapsedTime = time()-startTime

print(f"Elapsed time = {int(elapsedTime//60)}:{int(elapsedTime%60)}")

