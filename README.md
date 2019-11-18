# Senac_Ep2

docker-compose up -d

docker exec -it hadoop bash

cd volume

hdfs dfs -mkdir ep3

hdfs dfs -mkdir ep3/input

hdfs dfs -mkdir ep3/output

# A planilha teste1.csv contém a mesma estrutura da planilha yellow_tripdata_2019-01.csv porém com uma quantidade menor de registro para não ultrapassar a limite da VM
hdfs dfs -put teste1.csv ep3/input

# Somar total de cobranças por dia
python taxi1_ep3.py hdfs:///user/root/ep3/input/* -r hadoop

# Tempo médio de corrida por dia em minutos
python taxi2_ep3.py hdfs:///user/root/ep3/input/* -r hadoop

# Valor médio de corrida a cada 15 minutos
