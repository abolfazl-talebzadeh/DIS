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
        date = lineList[14]
        if date.strip() !="review_date":
            yield date[:7],1
        yield "", 0

    def combiner_count(self, key, values):
        yield key, sum(values)
        
    def reducer_count(self, key, values):
        yield key, sum(values)
        
    def mapper_avg(self, key, value):
        sdate = key.split("-")
        yield sdate[0], value

    def reducer_avg(self, key, values):
        values_list = list(values)
        if len(values_list)<1:
            yield "No values", 0
        maxOut = "Hotest month on "+str(key)
        minOut = "Coldest month on "+str(key)
        minList = [values_list.index(min(values_list))+1,min(values_list)]
        maxList = [values_list.index(max(values_list))+1,max(values_list)]
        yield maxOut, maxList
        yield minOut, minList

if __name__ == '__main__':
    MRCountSumAvg.run()