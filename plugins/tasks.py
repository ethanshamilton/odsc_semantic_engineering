# tasks.py
# ehamilton@enterprise-knowledge.com
# ---
# This file contains definitions of custom Airflow tasks. 
# These functions must be called directly by DAGs; any functions that are used but not called
# directly from a DAG should go into helpers.py. Any functions that deal with external integrations
# should go into their respective clients.
# ---
import os
import csv
import json
import logging
import helpers as h
import pandas as pd
from itertools import islice
from datetime import datetime
import GraphDBClient as gdb
from rdflib.namespace import RDF, SKOS
from rdflib import Graph, Namespace, Literal, URIRef

def clear_existing_graphs(gdb_url:str, gdb_repo:str, targets:list[str], auth:tuple=("", "")) -> None:
    """ Any named graphs including a target string in the name will be deleted via SPARQL.
    - `gdb_url`: URL of GraphDB Endpoint
    - `gdb_repo`: Target GraphDB Repository
    - `targets`: A list of strings where each string is a way to identify a named graph that should be deleted. 
    - `auth`: GraphDB Username and Password as a tuple. 
    """
    for s in targets:
        filter_block = f"FILTER (CONTAINS(STR(?graph), '{s}'))"
        
        q1 = f"""
        SELECT DISTINCT ?graph
        WHERE {{
            GRAPH ?graph {{
                ?s ?p ?o .
            }}
            {filter_block}
        }}
        """
        logging.info(q1)
        q1_result = gdb.send_query(gdb_url, gdb_repo, q1, auth)
        logging.info(q1_result)
        if q1_result:
            named_graphs = q1_result['results']['bindings']
        else:
            logging.info("No matching graphs detected.")
            pass
        
        if q1_result:
            for named_graph in named_graphs:
                q2 = f"DROP GRAPH <{named_graph['graph']['value']}>"
                q2_result = gdb.send_query(gdb_url, gdb_repo, q2, auth, is_update=True)
                logging.info(q2_result)
        else:
            pass

def clear_local_data(path:str):
    """ Deletes all files in a directory. """
    # Check if the path exists and is a directory
    if os.path.exists(path) and os.path.isdir(path):
        # Iterate over all files in the directory
        for file_name in os.listdir(path):
            file_path = os.path.join(path, file_name)
            # Check if the item is a file (not a directory)
            if os.path.isfile(file_path):
                # Delete the file
                os.remove(file_path)
        print(f"All files in directory '{path}' have been deleted.")
    else:
        print(f"The specified path '{path}' does not exist or is not a directory.")

def convert_taxonomy_to_rdf(input_path:str, debug=False):
    """ This function will take a CSV file containing altLabels and a taxonomy
    represented in tree-style and convert it to RDF. 
    """
    ### 1 - Get the data from CSV and prepare it for processing.
    altlabels = []
    taxo_tree = []
    # Read data from the CSV and convert it into necessary variables.
    with open(input_path, 'r') as file:
        csv_reader = csv.reader(file)
        header = next(csv_reader)
        for row in csv_reader:
            if row[0]:
                alt_row = row[0].split(';')
                alt_row = [altlabel.strip() for altlabel in alt_row if altlabel.strip()]  # Filters out any empty strings
                altlabels.append(alt_row)
            else:
                altlabels.append([])
            # Create taxo_tree matrix
            taxo_tree.append(row[1:])

    ### 2 - Process the taxonomy tree
    g = Graph()
    ns = Namespace('http://www.ek.net/rec_subject/')
    REC_SUBJECT_TYPE = URIRef("http://www.ek.net/model#rec_subject")
    g.bind('ekt', ns)

    def create_triples(graph, matrix, altlabels, parent=None):
        for row_index, row in enumerate(matrix):
            for col, term in enumerate(row):
                if term:
                    term_uri = URIRef(h.create_unique_uri(term.replace(" ", "_"), ns))
                    graph.add((term_uri, SKOS.prefLabel, Literal(term)))
                    graph.add((term_uri, RDF.type, REC_SUBJECT_TYPE))
                    for alt_label in altlabels[row_index]:
                        graph.add((term_uri, SKOS.altLabel, Literal(alt_label)))
                    if parent:
                        graph.add((term_uri, SKOS.broader, parent))
                        graph.add((parent, SKOS.narrower, term_uri))
                    create_triples(graph, [r[col+1:] for r in matrix[row_index+1:] if col+1 < len(r)],
                                [altlabels[i] for i in range(row_index+1, len(matrix))], parent=term_uri)

    create_triples(g, taxo_tree, altlabels)

    if debug:
        print(g.serialize(format='turtle'))
    else:
        now = datetime.now()
        with open(now.strftime('data/output/taxonomy-%Y_%m_%d-%H_%M_%S.ttl'), 'w') as f:
            f.write(g.serialize(format='turtle'))
            print("Taxonomy Processing Complete")

def create_rdf_tag_graph(input_content_file:str, gdb_endpoint:str, gdb_repo:str) -> None:
    """ Given a JSON file containing text data, run the auto-tagging process and produce
      a batch of .ttl files as output. """
    ### clear the output directory
    directory = "data/output/ttl/"
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            if filename.startswith("tagged_content"):
                os.remove(os.path.join(directory, filename))

    ### set up
    BATCH_SIZE = 10
    EKC = Namespace("http://www.ek.net/content/")
    g = Graph()
    g.bind('ekc', EKC)

    ### load data
    with open(input_content_file, 'r') as file:
        data = json.load(file)
    
    total_files = len(data)
    logging.info(f"# Files: {total_files}")
    
    ### main loop
    index = 0  # Starting index for each batch
    while index < len(data):
        batch = data[index:index + BATCH_SIZE]
        if not batch:
            break

        for file in batch:
            # transform data into content item and other processing
            content_item = h.create_content_item(file)
            segments = h.process_content_item(content_item, gdb_endpoint, gdb_repo)
            h.add_content_to_graph(g, content_item, segments)

        index += BATCH_SIZE  # Move the start index to the next batch
    
    ### OUTPUT GRAPH
    now = datetime.now()
    graph_filename = now.strftime("tagged_content-%Y%m%d_%H%M%S.ttl")
    g.serialize(destination=f'data/output/ttl/{graph_filename}', format='turtle')