# ~ content_pipeline.py
# ehamilton@enterprise-knowledge.com
# ---
# The content pipeline DAG pulls a set of content from UCR specified by servicelines, transforms the data into a graph structure, applies tags, and sends the data to GraphDB. 
# ---
## IMPORTS
### 3rd-party
import os
import logging
from airflow import DAG
from datetime import datetime
from dotenv import load_dotenv
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
### custom
from tasks import create_rdf_tag_graph
from GraphDBClient import post_to_graphdb

## INIT ENV
### Environment Variables
logging.basicConfig(level=logging.INFO)
load_dotenv()
now = datetime.now()
NAMED_GRAPH_DESTINATION = now.strftime("content_pipeline_job-%Y_%m_%d__%H_%M")
GDB_ENDPOINT = os.getenv("GDB_URL")
GDB_REPO = os.getenv("GDB_REPO")
GDB_USERNAME = os.getenv("GDB_USERNAME")
GDB_PASSWORD = os.getenv("GDB_PASSWORD")
NO_PROXY=os.getenv("NO_PROXY")
### Global Constants
INPUT_CONTENT_FILE = "data/input/articles_data.json"
RDF_PATH = "data/output/ttl"
RDF_PREFIX = "tagged_content"

## INIT DAG
dag = DAG(
    'content_pipeline',
    default_args={
        'owner': 'ehamilton',
    },
    description='The content pipeline DAG takes input content from a source file, transforms the data into a graph structure, applies tags, and sends the data to GraphDB.',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
)

## TRANSFORM DATA
create_rdf_graph = PythonOperator(
    task_id="create_rdf_graph",
    python_callable=create_rdf_tag_graph,
    op_args=[
        INPUT_CONTENT_FILE,
        GDB_ENDPOINT,
        GDB_REPO
    ],
    dag=dag
)

## LOAD DATA TO GRAPHDB
load_to_graphdb = PythonOperator(
    task_id="load_to_graphdb",
    python_callable=post_to_graphdb,
    op_args=[
        GDB_ENDPOINT, 
        GDB_REPO, 
        NAMED_GRAPH_DESTINATION,
        RDF_PATH,
        RDF_PREFIX,
        (GDB_USERNAME, GDB_PASSWORD) 
    ],
    dag=dag
)

create_rdf_graph >> load_to_graphdb