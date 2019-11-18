from mrjob.job import MRJob
from mrjob.step import MRStep

class TaxiNYC(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.busca_valor, combiner=self.soma_valor, reducer=self.media_final)
        ]
    
    def busca_valor(self, _, line):
        colunas = [s.strip('"') for s in line.split(',')]
        if colunas[16] != 'total_amount':
            yield colunas[1][0:10], float(colunas[16])

    def soma_valor(self, key, values):
        yield count(key), sum(values)
            
    def media15min(self, dias, total_valor):
        minutos_media = ((dias * 24 * 60) / 15)
        media = total_valor / minutos_media
        return None, media            
            
    def media_final(self, key, values):
        yield self.media15min(key, sum(values))

if __name__ == '__main__':
    TaxiNYC.run()
