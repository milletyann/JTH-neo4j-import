from neo4j import GraphDatabase
from credentials import read_instance_credentials

def delete_graph(batch_size=5000):

    creds = read_instance_credentials()
    uri = creds["NEO4J_URI"]
    username = creds["NEO4J_USERNAME"]
    password = creds["NEO4J_PASSWORD"]

    driver = GraphDatabase.driver(uri, auth=(username, password))
    
    with driver.session() as session:
        while True:
            result = session.run("MATCH (n) WITH n LIMIT $batch_size DETACH DELETE n RETURN count(n) AS deleted", batch_size=batch_size)
            deleted_count = result.single()["deleted"]

            if deleted_count == 0:
                break
    driver.close()


if __name__ == '__main__':
    delete_graph()
