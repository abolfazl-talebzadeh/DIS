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
        line = line.strip() # remove leading and trailing whitespace
        lineList = line.split("\t")
        date = lineList[14]
       # dataList = date.split("-")
        if date.strip() !="review_date":#len(date)==10:
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
        avgOut = "Average reviews per month on "+str(key)
        maxOut = "Maximum reviews per month on "+str(key)
        minOut = "Minimum reviews per month on "+str(key)
        yield avgOut, sum(values_list)/len(values_list)
        yield maxOut, max(values_list)
        yield minOut, min(values_list)

if __name__ == '__main__':
    MRCountSumAvg.run()