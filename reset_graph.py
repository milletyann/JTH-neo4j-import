from neo4j import GraphDatabase
from credentials import read_instance_credentials

def delete_graph():

    creds = read_instance_credentials()
    uri = creds["NEO4J_URI"]
    username = creds["NEO4J_USERNAME"]
    password = creds["NEO4J_PASSWORD"]

    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")
    
    driver.close()


if __name__ == '__main__':
    delete_graph()