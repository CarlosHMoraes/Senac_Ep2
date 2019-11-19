from mrjob.job import MRJob
from mrjob.step import MRStep

class TaxiNYC(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.busca_valor, combiner=self.soma_valor, reducer=self.media_final),
            MRStep(reducer=self.media_final_2)
        ]
    
    def busca_valor(self, _, line):
        colunas = [s.strip('"') for s in line.split(',')]
        if colunas[16] != 'total_amount':
            yield colunas[1][0:10], float(colunas[16])

    def soma_valor(self, key, values):
        yield key, sum(values)
            
    def media15min(self, dias, valores):
        total_valor = 0
        for valor in valores: 
            total_valor += valor / ((24 * 60) / 15)
        return None, total_valor            
            
    def media_final(self, key, values):
        yield self.media15min(key, values)

    def media(self, dias, valores):
        count, total_valor = 0, 0
        for valor in valores: 
            total_valor+= valor
            count+=1
        return None, (total_valor/count)  

    def media_final_2(self, key, values):
        yield self.media(key, values)

if __name__ == '__main__':
    TaxiNYC.run()
