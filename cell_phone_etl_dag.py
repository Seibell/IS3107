from datetime import datetime, timedelta
from pymongo import MongoClient
import json
import re
from bson import json_util

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def cell_phone_etl():
    # Connect to MongoDB
    client = MongoClient('localhost', 27017)
    mydb = client["IS3107Project"]
    my_collection = mydb["phonesNaccessories"]

    # Aggregate Queries
    ## Remove all irrelevant fields
    q0 = {"$unset" : ["fit","tech1","tech2","imageURL","imageURLHighRes"]}

    ## Match all documents that have "Cell Phones" string in category field
    q1 = {"$match" :{ "category": { "$in": ["Cell Phones"] } } }

    ## Match all documents that have some sort of ranking in the general Cell Phones & Accessories
    regexQ2 = re.compile("^>#.* in Cell Phones & Accessories \(See Top 100 in Cell Phones & Accessories\)")
    q2 = {"$match": { "rank": { "$elemMatch": {"$regex": regexQ2 } } } }

    ## Add a new field ranking the documents based on the "rank" string. Removes all hashtags and commas.
    regexQ3 = re.compile("[#,]")
    q3 = {"$addFields": {"overallRankRemovedHash": {"$replaceAll": {"input": {"$arrayElemAt": ["$rank", 0]} , "find": "#", "replacement": ""}}}}

    q3p1 = {"$addFields": {"overallRankRemovedRightArrow": {"$replaceAll": {"input": "$overallRankRemovedHash", "find": ">", "replacement" : ""}}}}
    q3p2 = {"$addFields": {"overallRankRemovedComma": {"$replaceAll": {"input": "$overallRankRemovedRightArrow", "find": ",", "replacement": ""}}}}

    ## Now split by whitespace and only select first element (which is the number rank)
    q4 = {"$addFields": {"overallRankNumber": {"$toInt": {"$arrayElemAt": [{"$split": ["$overallRankRemovedComma", " "]}, 0] } } } }

    ## remove the intermediate "rankRemovedHashComma" (and also _id actually)
    q5 = {"$unset" : ["overallRankRemovedHash", "overallRankRemovedRightArrow", "overallRankRemovedComma","_id"]}

    ## sort by rank ascending
    sortQ = {"$sort" : {"overallRankNumber" : 1}}

    # pipeline
    aggPipe = [q0, q1, q2, q3, q3p1, q3p2, q4, q5, sortQ]

    # processing thru pipeline
    commandCursor = my_collection.aggregate(aggPipe)

    # put all results of cursor into dict
    documents = []
    for document in commandCursor:
        documents.append(document)

    with open('output.json', 'w') as outfile:
        json.dump(documents, outfile)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'cell_phone_etl_dag',
    default_args=default_args,
    description='Cell Phone ETL using MongoDB',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 16),
    catchup=False
)

etl_task = PythonOperator(
    task_id='cell_phone_etl',
    python_callable=cell_phone_etl,
    dag=dag
)

etl_task