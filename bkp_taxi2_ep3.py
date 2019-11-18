from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime


fmt = '%Y-%m-%d %H:%M:%S'

class TaxiNYC(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.busca_tempo,
                    #combiner=self.agrupa_tempo_qtd,
                    #combiner=self.agrupaX,
                    reducer=self.tempo_medio)
        ]

    def busca_tempo(self, _, line):
        colunas = [s.strip('"') for s in line.split(',')]
        if colunas[16] != 'total_amount':
            #d1 = datetime.strptime(colunas[1],fmt)
            #d2 = datetime.strptime(colunas[2],fmt)
            #yield ( colunas[1][0:9], (((d2-d1).days * 24 * 60), 1) )  
            #data = (d2-d1).days
            yield ( colunas[1], colunas[2])  

#    def agrupaX(self, dia, tempo_qtd):
#        for tempo, qtd in tempo_qtd:
#            yield (dia, (sum(tempo), sum(qtd)))

#    def agrupa_tempo_qtd(self, dia, tempo_qtd):
#        count, tempo_total = 0, 0
#        for tempo, qtd in tempo_qtd:
#            count += qtd
#            tempo_total += tempo
#        yield (dia, (tempo_total/count))

#    def tempo_medio(self, key, values):
#        yield key, values

#    def tempo_medio(self, key, values):
#        for tempo, qtd in values:
#            yield key, (tempo/qtd)

    def tempo_medio(self, dia, tempo):
        yield dia, sum(tempo)

if __name__ == '__main__':
    TaxiNYC.run()