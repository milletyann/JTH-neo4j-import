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
            print("✅ Constraint created successfully.")
        except Neo4jError as e:
            if "already exists" in str(e):
                print("ℹ️ Constraint already exists.")
            else:
                print(f"❌ Failed to create constraint: {e}")
        
        try:
            result = session.run("CALL n10s.graphconfig.init()")
            print("✅ n10s.graphconfig.init() called successfully.")
            for record in result:
                print("Returned config:", dict(record))
        except Neo4jError as e:
            print(f"❌ Failed to initialize Neosemantics: {e}")
    
    driver.close()
