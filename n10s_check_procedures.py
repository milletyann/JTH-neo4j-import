import os
import sys
import argparse
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from connect_to_db import connect_to_db

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    
    parser.add_argument('db_loc', type=str, help="Database location (cloud or local)")
    args = parser.parse_args()
    db_loc = args.db_loc
    
    driver = connect_to_db(db_loc)

    with driver.session() as session:
        try:
            # Test calling a known n10s procedure
#            result = session.run("CALL n10s.graphconfig.show()")
            result = session.run("SHOW PROCEDURES YIELD name WHERE name STARTS WITH 'n10s' RETURN name ORDER BY name")
            print("Plugin n10s dispo. Config:")
            for record in result:
                print(f"  - {record['name']}")
        except Neo4jError as e:
            print("Plugin n10s pas dispo ou erreur.")
            print("Erreur:", e)

        driver.close()
