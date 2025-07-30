from neo4j import GraphDatabase
from credentials import read_instance_credentials


def connect_to_db(db_loc):
    print("Connecting to AuraDB instance...")
    creds = read_instance_credentials(db_loc)
    uri = creds["NEO4J_URI"]
    username = creds["NEO4J_USERNAME"]
    password = creds["NEO4J_PASSWORD"]
    
    driver = GraphDatabase.driver(
        uri,
        auth=(username, password),
        connection_timeout=120
    )
    
    return driver