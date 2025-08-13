import csv
from rdflib import Graph, Namespace, Literal, URIRef, BNode
from rdflib.namespace import RDF, XSD
import re
import json
import ast

files = {
    "candidates": "JTH/candidates.csv",
    "jobs": "JTH/jobs.csv",
    "applications": "JTH/applications.csv",
}

NS = Namespace("http://jth-tsp.org/")

g = Graph()
g.bind("jth", NS)


# def safe_uri(s):
#     return s.strip().replace(" ", "_").replace("/", "_").replace(",", "_")

def parse_json(val):
    try:
        data = json.loads(val)
    except json.JSONDecodeError:
        try:
            data = ast.literal_eval(val)
        except (ValueError, SyntaxError):
            data = None
    
    return data

def safe_uri(s):
    return re.sub(r'[^a-zA-Z0-9]', '_', s)

# --- CANDIDATES ---
with open(files['candidates']) as f:
    reader = csv.DictReader(f)
    for row in reader:
        cand_uri = NS["Candidate_" + row["candidate_id"]]
        g.add((cand_uri, RDF.type, NS.Candidate))
        
        # FOR EVERY CANDIDATE
        for col, val in row.items():
            if col == "candidate_id" or not val.strip():
                continue

            # JSON
            if val.strip().startswith("[") or val.strip().startswith("{"):
                try:
                    data = json.loads(val)
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                node = URIRef(NS[f"{col}_{safe_uri(item.get('name','item'))}"])
                                g.add((cand_uri, NS[col], node))
                                for k, v in item.items():
                                    g.add((node, NS[k], Literal(v)))
                            else:
                                g.add((cand_uri, NS[col], Literal(item)))
                    elif isinstance(data, dict):
                        node = URIRef(NS[f"{col}_{safe_uri(data.get('name','item'))}"])
                        g.add((cand_uri, NS[col], node))
                        for k, v in data.items():
                            g.add((node, NS[k], Literal(v)))
                except json.JSONDecodeError:
                    g.add((cand_uri, NS[col], Literal(val)))

            # Liste à point-virgule
            elif ";" in val:
                items = [x.strip() for x in val.split(";") if x.strip()]
                for item in items:
                    obj_uri = NS[f"{col}_{safe_uri(item)}"]
                    g.add((obj_uri, RDF.type, NS[col.capitalize()]))
                    g.add((cand_uri, NS[col], obj_uri))

            # Liste à virgule
            elif "," in val:
                items = [x.strip() for x in val.split(",") if x.strip()]
                for item in items:
                    obj_uri = NS[f"{col}_{safe_uri(item)}"]
                    g.add((obj_uri, RDF.type, NS[col.capitalize()]))
                    g.add((cand_uri, NS[col], obj_uri))

            # Litéraux
            else:
                g.add((cand_uri, NS[col], Literal(val)))

# --- JOBS ---
JSON_COMPLEX_COLUMNS = ['llm_required_languages_spoken']
with open(files['jobs']) as f:
    reader = csv.DictReader(f)
    for row in reader:
        job_uri = NS["Job_" + row["job_id"]]
        g.add((job_uri, RDF.type, NS.Job))
        
        # FOR EVERY JOB
        for col, val in row.items():
            if col == "job_id" or not val.strip():
                continue

            # JSON
            if col in JSON_COMPLEX_COLUMNS:
                try:
                    data = json.loads(val.replace("'", '"'))  # Ensure valid JSON
                    if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                        for item in data:
                            bnode = BNode()
                            g.add((job_uri, NS[col], bnode))
                            if "name" in item:
                                g.add((bnode, NS.languageName, Literal(item["name"])))
                            if "level" in item:
                                g.add((bnode, NS.level, Literal(item["level"])))
                except json.JSONDecodeError:
                    # If not valid JSON, just store as literal
                    g.add((job_uri, NS[col], Literal(val)))
            elif val.strip().startswith("[") or val.strip().startswith("{"):
                try:
                    data = json.loads(val)
                    if isinstance(data, list):
                        for item in data:
                            if isinstance(item, dict):
                                #node = NS[f"{col}_{safe_uri(item.get('name','item'))}"]
                                node = URIRef(NS[f"{col}_{safe_uri(item.get('name','item'))}"])
                                g.add((job_uri, NS[col], node))
                                for k, v in item.items():
                                    g.add((node, NS[k], Literal(v)))
                            else:
                                g.add((job_uri, NS[col], Literal(item)))
                    elif isinstance(data, dict):
                        node = NS[f"{col}_{safe_uri(data.get('name','item'))}"]
                        g.add((job_uri, NS[col], node))
                        for k, v in data.items():
                            g.add((node, NS[k], Literal(v)))
                except json.JSONDecodeError:
                    g.add((job_uri, NS[col], Literal(val)))

            # Liste à point-virgules
            elif ";" in val:
                items = [x.strip() for x in val.split(";") if x.strip()]
                for item in items:
                    obj_uri = NS[f"{col}_{safe_uri(item)}"]
                    g.add((obj_uri, RDF.type, NS[col.capitalize()]))
                    g.add((job_uri, NS[col], obj_uri))

            # Liste à virgule
            elif "," in val:
                items = [x.strip() for x in val.split(",") if x.strip()]
                for item in items:
                    obj_uri = NS[f"{col}_{safe_uri(item)}"]
                    g.add((obj_uri, RDF.type, NS[col.capitalize()]))
                    g.add((job_uri, NS[col], obj_uri))

            # Litéraux
            else:
                g.add((job_uri, NS[col], Literal(val)))

# --- APPLICATIONS ---
with open(files['applications']) as f:
    reader = csv.DictReader(f)
    for row in reader:
        app_uri = NS["Application_" + row["application_id"]]
        cand_uri = NS["Candidate_" + row["candidate_id"]]
        job_uri = NS["Job_" + row["job_id"]]
        
        g.add((app_uri, RDF.type, NS.Application))
        g.add((app_uri, NS.applicationOf, cand_uri))
        g.add((app_uri, NS.applicationFor, job_uri))
        
        for col, val in row.items():
            if col in ["application_id", "candidate_id", "job_id"] or not val:
                continue
            
            # Detect date columns
            if "date" in col.lower():
                g.add((app_uri, NS[col], Literal(val, datatype=XSD.date)))
            else:
                g.add((app_uri, NS[col], Literal(val)))

# Save RDF
g.serialize("graph.ttl", format="turtle")
