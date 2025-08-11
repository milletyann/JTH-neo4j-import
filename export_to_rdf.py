from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from connect_to_db import connect_to_db

driver = connect_to_db(db_loc='local')

with driver.session() as session:
    try:
        #result = session.run("CALL n10s.rdf.stream.fetch('Turtle') YIELD rdf")
        result = session.run("""
            CALL n10s.rdf.export.cypher('Turtle', { cypher: 'MATCH (n)-[r]->(m) RETURN n,r,m', format: "RDF/XML" }) YIELD rdf
        """)

        rdf_lines = [record["rdf"] for record in result]

        if rdf_lines:
            with open("exported_graph.ttl", "w", encoding="utf-8") as f:
                for line in rdf_lines:
                    f.write(line + "\n")
            print(f"✅ RDF export complete. {len(rdf_lines)} triples written to exported_graph.ttl")
        else:
            print("⚠️ RDF export returned no data (empty graph?)")

    except Exception as e:
        print("❌ Error during RDF export:", e)

driver.close()
