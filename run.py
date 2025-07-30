import argparse
from connect_to_db import connect_to_db
from graph_build import graph_build
from allen import allen
from enrich_nodes import enrich_nodes

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('db_loc', type=str, help="Database location (cloud or local)")
    args = parser.parse_args()
    db_loc = args.db_loc
    
    driver = connect_to_db(db_loc)
    print("Connection established with db!")

    print("Create graph nodes and relations...")
    graph_build(driver)
    print("Computing Allen Predicates...")
    allen(driver)
    print("Enrich nodes with properties")
    enrich_nodes(driver)
    
    # close driver
    driver.close()