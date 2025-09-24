from rdflib import Graph, URIRef, Literal
from neo4j import GraphDatabase
import re

# ---- FILE ----
RDF_FILE = "graph_extract.ttl"

# ---- FUNCTIONS ----
def read_instance_credentials():
    creds = {}
    path = f"credentials_cloud.txt"

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            if "=" in line:
                key, value = line.split("=", 1)
                creds[key.strip()] = value.strip()

    return creds

def load_rdf_graph(ttl_file):
    g = Graph()
    g.parse(ttl_file, format="turtle")
    return g

def connect_driver(uri, user, password):
    return GraphDatabase.driver(uri, auth=(user, password))

def create_constraints(session):
    session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (n:RDFResource) REQUIRE n.uri IS UNIQUE")
    
def safe_property_name(iri: str) -> str:
    # Cut the full uri address and take the last part
    return re.sub(r'[^a-zA-Z0-9_]', '_', iri.split('/')[-1])

# def safe_key(uri: str) -> str:
#     key = uri.split("#")[-1].split("/")[-1]
#     key = re.sub(r'\W+', '_', key)
#     return key

def add_triple(session, s, p, o):
    pred = str(p)

    if isinstance(s, URIRef):
        session.run("MERGE (sub:RDFResource {uri: $uri})", uri=str(s))

        if isinstance(o, URIRef):
            # Create relationship to another resource
            session.run("""
                MATCH (sub:RDFResource {uri: $suri})
                MERGE (obj:RDFResource {uri: $ouri})
                MERGE (sub)-[r:REL {predicate: $pred}]->(obj)
            """, suri=str(s), ouri=str(o), pred=pred)

        elif isinstance(o, Literal):
            # Add property directly on the subject node
            prop_key = safe_property_name(pred)
            session.run(f"""
                MATCH (sub:RDFResource {{uri: $suri}})
                SET sub.{prop_key} = $val
            """, suri=str(s), val=str(o))
            
# def add_triple(session, s, p, o):
#     pred_key = safe_key(str(p))

#     if isinstance(o, Literal):
#         # Add property to subject node
#         session.run(f"""
#             MERGE (sub:RDFResource {{uri: $suri}})
#             SET sub.{pred_key} = $val
#         """, suri=str(s), val=str(o))

#     elif isinstance(o, URIRef):
#         # Create relationship
#         session.run(f"""
#             MERGE (sub:RDFResource {{uri: $suri}})
#             MERGE (obj:RDFResource {{uri: $ouri}})
#             MERGE (sub)-[r:{pred_key}]->(obj)
#         """, suri=str(s), ouri=str(o))

# ---- MAIN ----
if __name__ == "__main__":
    rdf_graph = load_rdf_graph(RDF_FILE)
    
    creds = read_instance_credentials()
    uri = creds["NEO4J_URI"]
    username = creds["NEO4J_USERNAME"]
    password = creds["NEO4J_PASSWORD"]

    driver = connect_driver(uri, username, password)

    with driver.session() as session:
        create_constraints(session)

        print(f"Uploading {len(rdf_graph)} triples...")
        for s, p, o in rdf_graph:
            add_triple(session, s, p, o)

    driver.close()
    print("RDF graph imported in Neo4j.")