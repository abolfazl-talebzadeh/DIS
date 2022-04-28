from mrjob.job import MRJob
import csv
import re
import statistics

class MRCountSum(MRJob):
    def mapper(self, _, line):
        r = {}
        counter = 0
        score = 0
        resourceList = []
        with open("/home/ubuntu/AFINN-111.txt", 'r') as f:
            data = csv.reader(f,delimiter='\t')
            resourceList= list(data)
        for l in resourceList:
            r[l[0]]=float(l[1])
        wordDic={}
        line = line.strip() # remove leading and trailing whitespace
        lineList = line.split("\t")
        if isinstance(lineList[13],str)!=True or isinstance(lineList[14],str)!=True:
            return "invalid",0
        country = lineList[13]
        date = lineList[14]
        date = date[0:7]
        #cleanText = re.sub(r"[0-9(-_:;,.!?@#$%^&*)']",'',country.lower())
        cleanText = ''.join(w.lower() for w in country if w.isalpha() or w == " ")
        textList = cleanText.split(" ")
        t = country.lower()
        textList = t.split(" ")
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
        yield  date, out
        
    def reducer(self, key, values):
        vList = list(values)
        #m = sum(vList)/len(vList)
        m = statistics.mean(vList)
        yield key, m

if __name__ == '__main__':
    MRCountSum.run()