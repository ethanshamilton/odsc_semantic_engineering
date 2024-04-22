# helpers.py
# ehamilton@enterprise-knowledge.com
# ---
# This module contains helper functions for other code in the EK Semantic Recommender
# ---
import re
import uuid
import hashlib
import GraphDBClient as gdb
from rdflib import Graph, Namespace, Literal, URIRef

## CLASSES
class ContentItem:
    """ A container for all data pertaining to a content item. """
    def __init__(
            self, 
            title: str,
            tags: list[str],
            url: str,
            category: list[str],
            article_type: str,
            content: str
        ):
        self.uri = create_unique_uri()
        self.title = title
        self.tags = tags
        self.url = url
        self.category = category
        self.article_type = article_type
        self.content = content
        self.rdf_type = "http://www.ek.net/model#Content_Item"

    def __str__(self):
        return f"""
            URI: {self.uri}\n
            Title: {self.title}\n
            Tags: {self.tags}\n
            URL: {self.url}\n
            Category: {self.category}\n
            Article Type: {self.article_type}\n
            Content: \n
            {self.content}\n
        """
    
class ContentSegment:
    """ A container for data within a content segment. """
    def __init__(self, rdf_type: str, segment_data: str, tags: list[list, list], parent_uri: str):
        self.rdf_type = rdf_type
        self.segment_data = segment_data
        self.tags = tags
        self.parent_uri = parent_uri
        self.uri = create_unique_uri()

    def __str__(self):
        return f"URI: {self.uri}\nTags: {self.tags}\nData: {self.segment_data}\nType: {self.rdf_type}\nParent URI: {self.parent_uri}\n"

## FUNCTIONS
def add_content_to_graph(graph: Graph, content_item: ContentItem, content_segments: list[ContentSegment]):
    """ Add data from a content item and its segments to in-memory graph """
    ## NAMESPACES
    RDF = Namespace("http://www.w3.org/1999/02/22-rdf-syntax-ns#")
    RDFS = Namespace("http://www.w3.org/2000/01/rdf-schema#")
    SKOS = Namespace("http://www.w3.org/2004/02/skos/core#")
    ## RELATIONSHIPS
    IN_SEGMENT = URIRef("http://www.ek.net/model#inSegment")
    SEGMENT_OF = URIRef("http://www.ek.net/model#segmentOf")
    ## ATTRIBUTES
    ARTICLE_TYPE = URIRef("http://www.ek.net/model#articleType")
    URL = URIRef("http://www.ek.net/model#url")
    
    ## ADD CONTENT ITEM DATA
    content_item_node = URIRef(content_item.uri)
    content_item_type = URIRef(content_item.rdf_type)
    # create content_item properties/attributes
    graph.add((content_item_node, RDF.type, content_item_type))
    graph.add((content_item_node, ARTICLE_TYPE, Literal(content_item.article_type)))
    graph.add((content_item_node, URL, Literal(content_item.url)))

    ## ADD CONTENT SEGMENTS DATA
    for segment in content_segments:
        segment_node = URIRef(segment.uri)
        graph.add((segment_node, RDF.type, URIRef(segment.rdf_type)))
        graph.add((segment_node, RDF.value, Literal(segment.segment_data)))
        graph.add((segment_node, SEGMENT_OF, content_item_node))
        for tag in segment.tags:
            subject_node = URIRef(tag)
            graph.add((subject_node, IN_SEGMENT, URIRef(segment.uri)))

def create_content_item(data: dict) -> ContentItem:
    """ Input a dictionary and create a `ContentItem` object """
    # create content item
    title = data['title']
    tags = data['tags']
    url = data['url']
    category = data['categories']
    article_type = data['article_type']
    content = data['content']
    
    content_item = ContentItem(title, tags, url, category, article_type, content)
    return content_item

def create_segment(segment_type: str, data: str, parent_uri: str, gdb_endpoint:str, gdb_repo:str) -> ContentSegment:
    """ Tag content in a segment and assign a unique ID """
    ### Get terms for tagger
    taxonomy = gdb.get_taxonomy(gdb_endpoint, gdb_repo)
    
    ### Prepare tagger
    term_to_uri = {}
    for term in taxonomy:
        labels = term['labels'].split(", ")
        for label in labels: 
            term_to_uri[label.lower()] = term['uri']
    
    ### Create tags
    tags = set()
    words = data.lower().split()
    for word in words:
        if word in term_to_uri:
            tags.add(term_to_uri[word])
    
    ### Matching multi-word terms
    for term, uri in term_to_uri.items():
        if re.search(r'\b' + re.escape(term) + r'\b', data, re.IGNORECASE):
            tags.add(uri)

    content_segment = ContentSegment(segment_type, data, list(tags), parent_uri)
    print(content_segment)
    return content_segment

def create_unique_uri(identifier:str=None, namespace:str="http://ek.net/inst/", seeded:bool=False) -> str:
    """ 
    There are a few different ways to use this function:
        - If you already have something that can uniquely identify a resource, pass it in as identifier.
        - If you have something that can uniquely identify a resource but you would like a non-human-readable URI,
          pass the identifier and set seeded=True.
        - If you would like a randomly generated UUID, pass nothing to the function.
    """
    def generate_deterministic_uuid(seed, name):
        # Hash the string seed
        seed_hash = hashlib.sha256(seed.encode()).hexdigest()
        # Truncate to 32 characters to form a valid UUID
        truncated_hash = seed_hash[:32]
        
        # Create a UUID based on the truncated hash and name
        namespace = uuid.UUID(truncated_hash)
        return uuid.uuid5(namespace, name)

    if identifier and seeded:
        unique_id = generate_deterministic_uuid(namespace, identifier)
        return f"{namespace}{unique_id}"
    elif identifier:
        return f"{namespace}{identifier}"
    else:
        unique_id = uuid.uuid4()
        return f"{namespace}{unique_id}"
    
def process_content_item(content_item: ContentItem, gdb_endpoint:str, gdb_repo:str) -> list[ContentSegment]:
    """ Given a `ContentItem`, split into `ContentSegment` objects """
    # defining URIs
    parent_uri = content_item.uri
    title_uri = "http://www.ek.net/model#Title"
    tags_uri = "http://www.ek.net/model#Tags"
    categories_uri = "http://www.ek.net/model#Categories"
    text_uri = "http://www.ek.net/model#Text"

    # preparing data for tagging if necessary
    tags_string = ' '.join(content_item.tags)
    categories_string = ' '.join(content_item.category)

    # creating content segments
    title = create_segment(title_uri, content_item.title, parent_uri, gdb_endpoint, gdb_repo)
    tags = create_segment(tags_uri, tags_string, parent_uri, gdb_endpoint, gdb_repo)
    categories = create_segment(categories_uri, categories_string, parent_uri, gdb_endpoint, gdb_repo)
    content = create_segment(text_uri, content_item.content, parent_uri, gdb_endpoint, gdb_repo)

    return [title, tags, categories, content]