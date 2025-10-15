# Import JTH Dataset in Neo4j with Python

### Branch naming

In this branch we construct the graph in rdf format, query it (for correctness check) and possibly reconstruct it in an AuraDB instance (good construction check).

Query and AuraDB Reconstruction parts are not working yet.

### Requirements

- JTH Dataset with applications.csv (history of applications), candidates.csv, jobs.csv, all in a folder `JTH/`.
- Python libraries: neo4j, tqdm, math, pandas, datetime, itertools, numpy.

### How to build it

Run

```bash
python3 export_rdf.py
```

