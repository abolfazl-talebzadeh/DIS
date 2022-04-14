from mrjob.job import MRJob

class MRCountSum(MRJob):

    def mapper(self, _, line):
        line = line.strip() 
        lineList = line.split("\t")
        star = lineList[7]
        productTitle = lineList[5].strip()
        outputList = [productTitle,star]
        yield outputList,1

    def combiner(self, key, values):
        yield key, sum(values)
        
    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRCountSum.run()