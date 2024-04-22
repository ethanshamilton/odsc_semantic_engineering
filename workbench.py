# For testing stuff
import sys
sys.path.append('./plugins')
import os
import rdflib
from tqdm import tqdm
import networkx as nx
from dotenv import load_dotenv
from rdflib.namespace import RDF
import plugins.GraphDBClient as gdb
from rdflib import Graph, URIRef, Literal

load_dotenv()
GDB_ENDPOINT = os.getenv("GDB_URL")
GDB_REPO = os.getenv("GDB_REPO")
pathname = "data/input/sample_articles.json"

## GET DATA FROM GRAPHDB
query = """
SELECT ?s ?p ?o
WHERE { ?s ?p ?o . }
"""
data = gdb.send_query(GDB_ENDPOINT, GDB_REPO, query)
g = Graph()
for item in data['results']['bindings']:
    s = URIRef(item['s']['value'])
    p = URIRef(item['p']['value'])
    if item['o']['type'] == 'literal':
        o = Literal(item['o']['value'])
    else:
        o = URIRef(item['o']['value'])
    
    g.add((s, p, o))

## RECOMMENDER GRAPH
input = ['http://www.ek.net/rec_subject/Natural_Language_Processing']
weights = {"Title": 0.6, "Content": 1, "Category": 0.2, "Tags": 0.5}

def fetch_details(uri, graph):
    # Define a SPARQL query to fetch details based on URI
    query = f"""
    PREFIX ekm: <http://www.ek.net/model#>
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT ?titleLabel ?url
    WHERE {{
        ?title a ekm:Title .
        ?title ekm:segmentOf <{uri}> .
        ?title rdf:value ?titleLabel .
        <{uri}> ekm:url ?url .
    }}
    """
    # Execute the query and return results
    return list(graph.query(query))

def generate_recommendations_from_graph(rdf_graph, input_uris, weights, namespace_str='http://www.ek.net/model#'):
    
    def score_path(path, segment_weights, rdf_graph, namespace):
        # Example scoring function, customize as needed
        score = 0
        for node in path:
            if isinstance(node, rdflib.URIRef):
                type_uris = list(rdf_graph.objects(subject=node, predicate=RDF.type))
                for type_uri in type_uris:
                    score += segment_weights.get(type_uri, 0)
        score /= len(path) # Normalize score by path length, prioritizing shorter paths. 
        return score
    
    def rdflib_to_networkx_digraph(rdf_graph):
        g = nx.DiGraph()
        for s, p, o in rdf_graph:
            g.add_edge(s, o, key=p)
        return g
    
    # Define necessary namespaces
    namespace = rdflib.Namespace(namespace_str)
    
    # Convert RDF graph to NetworkX graph for path processing
    property_graph = rdflib_to_networkx_digraph(rdf_graph)
    
    # Define segment weights for content
    segment_weights = {
        rdflib.URIRef(namespace.Title): weights.get("Title", 1),
        rdflib.URIRef(namespace.Content): weights.get("Content", 1),
        rdflib.URIRef(namespace.Category): weights.get("Category", 1),
        rdflib.URIRef(namespace.Tags): weights.get("Tags", 1),
    }
    
    # Define output class URI (adjust as needed)
    output_content_class_uri = URIRef(namespace.Content_Item)
    
    # Query the RDF graph for output content URIs
    output_content_uris = list(rdf_graph.subjects(RDF.type, output_content_class_uri))
    
    # Store recommendations
    recommendations = []

    # Find paths and calculate scores for each output URI
    for output_uri in tqdm(output_content_uris, desc="Generating recommendations"):
        for input_uri in input_uris:
            paths = list(nx.all_simple_paths(property_graph, source=URIRef(input_uri), target=output_uri, cutoff=5))
            if paths:
                # Score paths and aggregate scores
                total_score = sum(score_path(path, segment_weights, rdf_graph, namespace) for path in paths)
                recommendations.append((output_uri, total_score, len(paths)))

    # Sort by the highest score
    recommendations.sort(key=lambda x: x[1])
    return recommendations

def display_recommendations(recommendations, rdf_graph):
    print("Recommendations (URI, Score, Path Count, Additional Info):")
    for uri, score, path_count in recommendations:
        details = fetch_details(uri, rdf_graph)
        details_str = ", ".join([f"{prop.n3(rdf_graph.namespace_manager)}: {val.n3()}" for prop, val in details])
        print(f"{uri}, Score: {score}, Path Count: {path_count}, Details: {details_str}")

recommendations = generate_recommendations_from_graph(g, input, weights)
display_recommendations(recommendations, g)