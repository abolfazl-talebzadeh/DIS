from mrjob.job import MRJob

class MRCountSum(MRJob):

    def mapper(self, _, line):
        line = line.strip() # remove leading and trailing whitespace
        lineList = line.split("\t")
        product = lineList[4]
        c = product[:4]
        yield c, 1
    def combiner(self, key, values):
        yield key, sum(values)
        
    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRCountSum.run()