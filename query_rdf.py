from rdflib import Graph

g = Graph()
g.parse("graph.ttl", format="turtle")

g.bind("jth", "http://jth-tsp.org/")
g.bind("xsd", "http://www.w3.org/2001/XMLSchema#")

query = """
    PREFIX jth: <http://jth-tsp.org/>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    
    SELECT DISTINCT ?candidate ?candidature WHERE {
    ?candidate a jth:Candidate ;
                jth:hasEvent ?event .
    ?event a ?eventType .
    FILTER(STRSTARTS(STR(?eventType), "2nd_interview_"))
    BIND(STRAFTER(STR(?eventType), "2nd_interview_") AS ?candidatureId)
    BIND(IRI(CONCAT(STR(jth:), ?candidatureId)) AS ?candidature)
    }
"""
results = g.query(query)

i = 0
print("SALUT")
for row in results:
    if i < 10:
        print(f"salut {i}")
        print(row.candidate, row.candidature)
    i += 1