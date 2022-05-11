from mrjob.job import MRJob

class MRCountSum(MRJob):

    def mapper(self, _, line):
        line = line.strip() 
        lineList = line.split("\t")
        country = lineList[0]
        yield country, 1
    def combiner(self, key, values):
        yield key, sum(values)
        
    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRCountSum.run()