# Senac_Ep2

docker-compose up -d

docker exec -it hadoop bash

cd volume

hdfs dfs -mkdir ep3

hdfs dfs -mkdir ep3/input

hdfs dfs -mkdir ep3/output

hdfs dfs -put teste1.csv ep3/input

python taxi1_ep3.py hdfs:///user/root/ep3/input/* -r hadoop

python taxi2_ep3.py hdfs:///user/root/ep3/input/* -r hadoop
