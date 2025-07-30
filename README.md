# Import JTH Dataset in Neo4j with Python

### Branch naming

In this branch, the graph model contains:

- 1 type of Event nodes (e:Event), with 1 type of relation (r:HAS_EVENT).
- Every information is contained in the nodes properties.


The graph is stored in a free AuraDB instance (only the difference in the code between same config `local` and `cloud` is the way to connect to the storage instance)

### Requirements

- JTH Dataset with applications.csv (history of applications), candidates.csv, jobs.csv.
- Python libraries: neo4j, graphdatascience, tqdm, math, pandas, datetime, itertools, numpy.

### How to build it

- Have an AuraDB instance create and empty (free tier is enough)
- Save the credentials .txt file in the root folder of the project
- In parallel, you can connect to the instance in the `Neo4j Desktop` > `Neo4j Browser` app to follow the changes and query the graph
- Run the following scripts in this order:

Create the nodes (Candidate, Event, Job) and link them.

```bash
python3 graph_build.py
```

Compute the Allen predicates between the Event nodes.

```bash
python3 allen.py
```

Complete the nodes properties.

```bash
python3 enrich_nodes.py
```

- If needed `reset_graph.py` and `reset_properties.py` respectively delete all the graph and removes the properties from Candidate and Job nodes.
