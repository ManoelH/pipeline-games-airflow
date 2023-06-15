from elasticsearch import helpers
from elasticsearch import Elasticsearch
import csv

es = Elasticsearch("http://localhost:9200")

def importDataToElastic():
       index_name = "ela"

       with open("/opt/airflow/dags/most_rated_games_2019.csv", "r") as fi:
           # Cria um array de dicionarios com os dados
           reader = csv.DictReader(fi, delimiter=",")
           actions = []
           for row in reader:
               # Objeto com informacoes
               action = {"index": {"_index": index_name, "_id": int(row[""])}}
               # O documento com os dados
               doc = {
                   "score": float(row["score"]),
                   "game": row["game"],
                   "genre": row["genre"],
                   "developer": row["developer"],
                   "year": int(row["year"])
                } 
               actions.append(action)
               actions.append(doc)
       es.bulk(index=index_name, operations=actions)
              
       result = es.count(index=index_name)
       print(result.body['count'])

importDataToElastic()       