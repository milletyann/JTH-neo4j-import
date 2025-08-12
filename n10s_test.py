import requests
import json
from credentials import read_instance_credentials

# --- Config ---
creds = read_instance_credentials('local')

NEO4J_URL = creds["NEO4J_URI"]  # or the correct host:port
AUTH = (creds["NEO4J_USERNAME"], creds["NEO4J_PASSWORD"])    # Replace with your username & password
EXPORT_PATH = "exported_graph.ttl"
RDF_FORMAT = "Turtle"  # Options: RDF/XML, Turtle, N-Triples, JSON-LD

# --- Cypher to export ---
cypher_query = """
MATCH p= ()-[r]->()
RETURN p
"""

# --- POST body ---
body = {
    "cypher": cypher_query,
    "format": RDF_FORMAT
}

# --- Send POST request ---
url = f"{NEO4J_URL}/rdf/1c1e1jnoaddnodes/cypher"
headers = {"Content-Type": "application/json"}

response = requests.post(url, headers=headers, auth=AUTH, data=json.dumps(body))

# --- Handle response ---
if response.status_code == 200:
    with open(EXPORT_PATH, "w", encoding="utf-8") as f:
        f.write(response.text)
    print(f"Export RDF réussie ({EXPORT_PATH})")
else:
    print(f"Export RDF raté ({response.status_code}):\n{response.text}")
