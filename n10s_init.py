import argparse
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from connect_to_db import connect_to_db

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument('db_loc', type=str, help="Database location (cloud or local)")
    args = parser.parse_args()
    db_loc = args.db_loc
    
    driver = connect_to_db(db_loc)

    with driver.session() as session:
        try:
            session.run("CREATE CONSTRAINT n10s_unique_uri FOR (r:Resource) REQUIRE r.uri IS UNIQUE")
            print("Contrainte réussie.")
        except Neo4jError as e:
            if "already exists" in str(e):
                print("Contrainte existe déjà.")
            else:
                print(f"Création de contrainte ratée: {e}")
        
        try:
            result = session.run("CALL n10s.graphconfig.init()")
            print("Appel réussi à n10s.graphconfig.init().")
            for record in result:
                print("Config:", dict(record))
        except Neo4jError as e:
            print(f"Initialisation de n10s: {e}")
    
    driver.close()
