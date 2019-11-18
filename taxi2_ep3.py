from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

fmt = '%Y-%m-%d %H:%M:%S'

class TaxiNYC(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.busca_tempo, reducer=self.tempo_medio)
        ]

    def busca_tempo(self, _, line):
        colunas = [s.strip('"') for s in line.split(',')]
        if colunas[16] != 'total_amount':
            d1 = datetime.strptime(colunas[1],fmt)
            d2 = datetime.strptime(colunas[2],fmt)
            minutos = (d2-d1).seconds / 60
            yield colunas[1][0:10], minutos

    def agrupaX(self, dia, tempo_qtd):
        count, tempo_total = 0, 0
        for tempo in tempo_qtd:
            count += 1
            tempo_total += tempo
        media = tempo_total/count
        return dia, media

    def tempo_medio(self, key, values):
        yield self.agrupaX(key, values)

if __name__ == '__main__':
    TaxiNYC.run()