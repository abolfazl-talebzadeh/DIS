from mrjob.job import MRJob
from mrjob.step import MRStep

class MRCountSumAvg(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_count,
                   combiner=self.combiner_count,
                   reducer=self.reducer_count),
            MRStep(mapper=self.mapper_avg,
                   reducer=self.reducer_avg)
            ]

    def mapper_count(self, _, line):
        line = line.strip() 
        lineList = line.split("\t")
        star = lineList[7]
        productTitle = lineList[5].strip()
        outputList = [productTitle,star]
        yield outputList,1

    def combiner_count(self, key, values):
        yield key, sum(values)
        
    def reducer_count(self, key, values):
        yield key, sum(values)
        
    def mapper_avg(self, key, value):
        if str(key[1]).isnumeric()== True:
            yield key[0], [int(key[1]),value]


    def reducer_avg(self, key, values):
        values_list = list(values)
        tempStar = 0
        tempNumber = 0
        for i in values_list:
            tempStar+=i[0]
            tempNumber+=i[1]
        yield key, tempStar/tempNumber

if __name__ == '__main__':
    MRCountSumAvg.run()