# GraphDBClient.py
# ehamilton@enterprise-knowledge.com
# ---
# Code for interacting with GraphDB
# ---
# ~ IMPORTS
import os
import logging
import requests

# ~ INTERNAL FUNCTIONS
def sparql_json_to_table(json_dict):
    """ Given the JSON output of a GraphDB SPARQL query, parse the results into table format. """
    headers = json_dict['head']['vars']
    rows = json_dict['results']['bindings']

    table = []
    table.append(headers)

    for row in rows:
        table_row = []
        for header in headers:
            table_row.append(row[header]['value'])
        table.append(table_row)

    return table

# ~ EXTERNAL FUNCTIONS
def get_taxonomy(endpoint:str, repo:str) -> dict:
    query = """
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX ekm: <http://www.ek.net/model#>
    select ?term (GROUP_CONCAT(?label; separator=", ") AS ?labels)
    where {
        ?term a ekm:rec_subject .

        { ?term skos:prefLabel ?label . }
        UNION
        { ?term skos:altLabel ?label . }
    }
    GROUP BY ?term
    """
    taxonomy_result = send_query(endpoint, repo, query)
    taxonomy = []
    for term in taxonomy_result['results']['bindings']:
        uri = term['term']['value']
        labels = term['labels']['value']
        row = {'uri': uri, 'labels': labels}
        taxonomy.append(row)
    return taxonomy

def post_to_graphdb(url:str, repo:str, context:str, rdf_path:str, rdf_prefix:str, auth:tuple=("username", "password")) -> None:
    """ 
    Given an endpoint, repo, named graph, and credentials this will post RDF files to GraphDB. 
    
    Params: 
    - `url`: URL for the target GraphDB instance
    - `repo`: ID for the target repo in GraphDB
    - `context`: ID for the named graph the data should be saved in. 
    - `rdf_path`: Directory of target data. 
    - `rdf_prefix`: String used to filter files for sending.
    - `auth`: Username and password combo for GraphDB. 
    """
    from helpers import create_unique_uri
    context = create_unique_uri(identifier=context)
    graphdb_url = f"{url}repositories/{repo}/statements?context=<{context}>"
    headers = {'Content-Type': 'application/x-turtle'}
    
    logging.info(f"URL: {url}\nRepo: {repo}\nContext: {context}\nRDF_PATH: {rdf_path}\n RDF_PREFIX: {rdf_prefix}")

    for filename in os.listdir(rdf_path):
        if filename.startswith(rdf_prefix):
            with open(f"{rdf_path}/{filename}", 'r') as f:
                rdf_data = f.read()
    
            response = requests.post(graphdb_url, data=rdf_data.encode('utf-8'), headers=headers, auth=auth)
    
            if response.status_code == 204:
                logging.info(f"Success")
            else:
                logging.error(f"Status code: {response.status_code}, Response: {response.text}")

def send_query(endpoint:str, repo:str, sparql_query:str, auth:tuple=("username", "password"), is_update:bool=False) -> dict:
        """ Given an endpoint, repo, query, and credentials this will run a SPARQL query in GraphDB and return a JSON response."""
        if is_update:
            url = f'{endpoint}repositories/{repo}/statements'  
            headers = {
                "Content-Type": "application/sparql-update",
                "Accept": "application/sparql-results+json"
            }
        else:
            url = f'{endpoint}repositories/{repo}'  
            headers = {
                "Content-Type": "application/sparql-query",
                "Accept": "application/sparql-results+json"
            }

        response = requests.post(url, data=sparql_query, headers=headers, auth=auth)
        
        if response.status_code == 200 or 204:
            try:
                data = response.json()
                return data
            except:
                pass
        else:
            logging.error(f"failed with status code: {response.status_code}")
            logging.error(f"{response.text}")
