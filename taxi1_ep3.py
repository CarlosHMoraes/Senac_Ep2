from mrjob.job import MRJob
from mrjob.step import MRStep

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