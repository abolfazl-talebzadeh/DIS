
from time import time


df = spark.read.csv("hdfs://group2:9000/dis_materials/1.tsv",header = 'true', sep = "\t")


def blobber(line): 
    from textblob import TextBlob 
    if isinstance(line[0], str)!=True or isinstance(line[1], str)!= True: 
        return "invalid", (0,0)
    date = line[1][0:7]
    cleanText = ''.join(w.lower() for w in line[0] if w.isalpha() or w == " ")
    b = TextBlob(cleanText) 
    s = b.sentiment.polarity 
    return date,(s,1) 


def m(x):
    if x[1][1]<1:
        return x[0],0
    return x[0],x[1][0]/x[1][1] 


startTime = time()

a = df.select("review_body","review_date")

b = a.rdd.map(blobber).reduceByKey(lambda e1,e2: (e1[0]+e2[0],e1[1]+e2[1]))

c = b.map(m)

d = c.toDF()

elapsedTime = time()-startTime

print(f"Elapsed time = {int(elapsedTime//60)}:{int(elapsedTime%60)}")

