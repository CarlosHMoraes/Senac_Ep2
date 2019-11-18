from mrjob.job import MRJob
from mrjob.step import MRStep

#class WordCount(MRJob):
#    def steps(self):
#        return [
#            MRStep(mapper=self.mapper, reducer=self.reducer)
#        ]
#
#    def mapper(self, _, line):
#        words = line.split(' ')
#        for word in words:
#            yield word, 1
#
#    def reducer(self, key, values):
#        yield key, sum(values)
#
#if __name__ == '__main__':
#    WordCount.run()
#(VendorID,tpep_pickup_datetime,tpep_dropoff_datetime,passenger_count,trip_distance,RatecodeID,store_and_fwd_flag,PULocationID,DOLocationID,payment_type,fare_amount,extra,mta_tax,tip_amount,tolls_amount,improvement_surcharge,total_amount,congestion_surcharge) = line.split(',')

class TaxiNYC(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.busca_valor, reducer=self.soma_valor)
        ]
    
    def busca_valor(self, _, line):
        colunas = [s.strip('"') for s in line.split(',')]
        if colunas[16] != 'total_amount':
            yield 'valor_total', float(colunas[16])

    def soma_valor(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    TaxiNYC.run()