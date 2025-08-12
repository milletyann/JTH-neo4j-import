# Import JTH Dataset in Neo4j with Python

### Branch naming

The graph can be constructed in several different ways:

- one or several event node types
- add informational nodes instead of keeping data in the node attributes

e.g. :

- `1cXe1j-addnodes` (1 type of candidate nodes, X types of event nodes, 1 type of job nodes, other information stored in additional nodes)
- `1c1e1j-noaddnodes` (1 type of candidate nodes, 1 type of event nodes, 1 type of job nodes, all other information as node properties)
- `1cXe1j-alladdnodes` (1 type of candidate nodes, X type of event nodes, 1 type of job nodes, all information are in additional nodes)

The only the difference in the code between same config `local` and `cloud` is the way to connect to the storage instance. `local` supposes that you have Neo4j Community or Entreprise edition installed and running on your machine, `cloud` suppose you have an instance (for instance in AuraDB) running on the cloud.

In both cases, the credentials file of the instance must be named `credentials_local.txt` or `credentials_cloud.txt` and have the format of the example file `credentials_example.txt` in the root folder of this repo.

### Requirements

- JTH Dataset with applications.csv (history of applications), candidates.csv, jobs.csv, all in a folder `JTH/`.
- Python libraries: neo4j, tqdm, math, pandas, datetime, itertools, numpy.

### How to build it

- Have an AuraDB instance create and empty
- Save the credentials .txt file in the root folder of the project
- In parallel, you can connect to the instance in the `Neo4j Desktop` > `Neo4j Browser` app to follow the changes and query the graph
- Run the following scripts in this order or just run `python3 run.py db_loc` db_loc is either `local`or `cloud`:

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
