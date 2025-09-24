import csv
from rdflib import Graph, Namespace, Literal, URIRef, BNode
from rdflib.namespace import RDF, XSD
from datetime import datetime
import re
import json
import ast

files = {
    "candidates": "JTH/candidates_extract.csv",
    "jobs": "JTH/jobs_extract.csv",
    "applications": "JTH/applications_extract.csv",
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
TO_SKIP_CAND_COL_VAL = {"": []}
with open(files['candidates']) as f:
    reader = csv.DictReader(f)
    for row in reader:
        cand_uri = NS["Candidate_" + row["candidate_id"]]
        g.add((cand_uri, RDF.type, NS.Candidate))
        
        # FOR EVERY CANDIDATE
        for col, val in row.items():
            if col == "candidate_id" or not val.strip():
                continue
            
            # SOME VALUES ARE DEFAULTS, DON'T CREATE TRIPLET IF MET
            if col in TO_SKIP_CAND_COL_VAL:
                skip_values = [str(v).strip().lower() for v in TO_SKIP_CAND_COL_VAL[col]]
                if val.strip().lower() in skip_values:
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
TO_SKIP_JOB_COL_VAL = {"": []}
with open(files['jobs']) as f:
    reader = csv.DictReader(f)
    for row in reader:
        job_uri = NS["Job_" + row["job_id"]]
        g.add((job_uri, RDF.type, NS.Job))
        
        # FOR EVERY JOB
        for col, val in row.items():
            if col == "job_id" or not val.strip():
                continue
            
            # SOME VALUES ARE DEFAULTS, DON'T CREATE TRIPLET IF MET
            if col in TO_SKIP_JOB_COL_VAL:
                skip_values = [str(v).strip().lower() for v in TO_SKIP_JOB_COL_VAL[col]]
                if val.strip().lower() in skip_values:
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
TO_SKIP_APP_COL_VAL = {"": []}
with open(files['applications']) as f:
    reader = csv.DictReader(f)
    for row in reader:
        app_uri = NS["Application_" + row["application_id"]]
        cand_uri = NS["Candidate_" + row["candidate_id"]]
        job_uri = NS["Job_" + row["job_id"]]
        
        g.add((app_uri, RDF.type, NS.Application))
        g.add((app_uri, NS.applicationOf, cand_uri))
        g.add((app_uri, NS.applicationFor, job_uri))

        event_nodes = []  # store (date_obj, event_uri)

        for col, val in row.items():
            if col in ["application_id", "candidate_id", "job_id"] or not val:
                continue

            # Skip default values
            if col in TO_SKIP_APP_COL_VAL:
                skip_values = [str(v).strip().lower() for v in TO_SKIP_APP_COL_VAL[col]]
                if val.strip().lower() in skip_values:
                    continue

            # Treat *_date columns as events
            if col.lower().endswith("_date"):
                event_name = col[:-5]  # remove "_date"
                event_uri = NS[f"{event_name}_{row['application_id']}"]
                
                g.add((event_uri, RDF.type, NS.Event))
                g.add((cand_uri, NS.hasEvent, event_uri))
                g.add((job_uri, NS.hasEvent, event_uri))
                
                g.add((event_uri, NS.date, Literal(val, datatype=XSD.date)))
                
                try:
                    date_obj = datetime.strptime(val, "%Y-%m-%d")
                    event_nodes.append((date_obj, event_uri))
                except ValueError:
                    pass  # skip if invalid date format
            
            else:
                # For now, handle non-date columns normally (unless last_stage_reached — handled later)
                if col != "last_stage_reached":
                    if "date" in col.lower():
                        g.add((app_uri, NS[col], Literal(val, datatype=XSD.date)))
                    else:
                        g.add((app_uri, NS[col], Literal(val)))

        # Attach last_stage_reached to the most recent event
        if row.get("last_stage_reached") and event_nodes:
            latest_event_uri = max(event_nodes, key=lambda x: x[0])[1]
            g.add((latest_event_uri, NS.lastEventReached, Literal(row["last_stage_reached"])))




# Save RDF
g.serialize("graph_extract.ttl", format="turtle")
