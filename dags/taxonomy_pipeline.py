# taxonomy_pipeline.py
# ehamilton@enterprise-knowledge.com
# ---
# This DAG will take a spreadsheet taxonomy, convert it to RDF, and push it to GraphDB. 
# ---
### STANDARD IMPORTS
import os
import logging
from airflow import DAG
from dotenv import load_dotenv
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
### CUSTOM IMPORTS
from GraphDBClient import post_to_graphdb
from tasks import clear_existing_graphs, convert_taxonomy_to_rdf, clear_local_data

logging.basicConfig(level=logging.INFO)

load_dotenv()
GDB_URL = os.getenv("GDB_URL")
GDB_REPO = os.getenv("GDB_REPO")
NO_PROXY = os.getenv("NO_PROXY")

now = datetime.now()
NAMED_GRAPH_DESTINATION = now.strftime("taxonomy_extraction-%Y_%m_%d-%H_%M_%S")
RDF_PATH = "data/output/"
RDF_PREFIX = "taxonomy"

# ~ Init DAG
dag = DAG(
    'taxonomy_pipeline',
    default_args={
        'owner': 'ehamilton',
    },
    description='This DAG will take a spreadsheet taxonomy, convert it to RDF, and push it to GraphDB.',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False
)

# ~ Clear taxonomy in GraphDB if exists
t1_clear_existing_graphs = PythonOperator(
    task_id="clear_graphdb_taxonomy",
    python_callable=clear_existing_graphs,
    op_args=[
        GDB_URL,
        GDB_REPO,
        ['taxonomy'], # Will remove any named graphs with this string in the name. 
    ],
    dag=dag
)

# ~ Convert taxonomy from spreadsheet to RDF
t2_convert_taxonomy_to_rdf = PythonOperator(
    task_id="convert_taxonomy_to_rdf",
    python_callable=convert_taxonomy_to_rdf,
    op_args=[
        'data/input/taxonomy.csv' # Path to taxonomy file. 
    ],
    dag=dag
)

# ~ Push taxonomy to GraphDB
t3_push_taxonomy_to_graphdb = PythonOperator(
    task_id="push_taxonomy_to_graphdb",
    python_callable=post_to_graphdb,
    op_args=[
        GDB_URL,
        GDB_REPO,
        NAMED_GRAPH_DESTINATION,
        RDF_PATH,
        RDF_PREFIX,
    ],
    dag=dag
)

t4_clear_local_data = PythonOperator(
    task_id="clear_local_data",
    python_callable=clear_local_data,
    op_args=[
        'data/output/'
    ],
    dag=dag
)

t1_clear_existing_graphs >> t2_convert_taxonomy_to_rdf >> t3_push_taxonomy_to_graphdb >> t4_clear_local_data