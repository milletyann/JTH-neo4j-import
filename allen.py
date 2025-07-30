import pandas as pd
from datetime import datetime
from tqdm import tqdm
from itertools import combinations
from connect_to_db import connect_to_db

# Récup events
def fetch_events(tx):
    query = """
    MATCH (e:Event)
    WHERE e.application_id IS NOT NULL AND e.timestamp IS NOT NULL
    RETURN elementId(e) AS neo4j_id, e.event_name AS event_name, e.application_id AS application_id, e.timestamp AS timestamp
    """
    result = tx.run(query)
    records = result.data()
    return pd.DataFrame(records)

# Calculer prédicats Allen
def allen_timestamps(t1, t2):
    if t1 < t2:
        return "BEFORE"
    elif t1 > t2:
        return "AFTER"
    else:
        return "EQUALS"

def allen_intervals(s1, e1, s2, e2):
    return

def compute_edges(group_df):
    edges = []
    for e1, e2 in combinations(group_df.itertuples(), 2):
        rel_type = allen_timestamps(e1.timestamp, e2.timestamp)
        inverse_rel = allen_timestamps(e2.timestamp, e1.timestamp)

        edges.append({
            "from_id": e1.neo4j_id,
            "to_id": e2.neo4j_id,
            "relation": rel_type
        })
        edges.append({
            "from_id": e2.neo4j_id,
            "to_id": e1.neo4j_id,
            "relation": inverse_rel
        })
    return edges

def create_allen_relationships(tx, edges):
    for edge in edges:
        tx.run("""
            MATCH (e1) WHERE elementId(e1) = $from_id
            MATCH (e2) WHERE elementId(e2) = $to_id
            MERGE (e1)-[r:%s]->(e2)
        """ % edge["relation"], from_id=edge["from_id"], to_id=edge["to_id"])

# La fonction à utiliser dans run.py
def allen(driver):
    with driver.session() as session:
        df_events = session.execute_read(fetch_events)

    df_events["timestamp"] = pd.to_datetime(df_events["timestamp"])
    print(f"Nombre total d'events récupérés: {len(df_events)}")

    grouped = df_events.groupby("application_id")
    all_app_ids = list(grouped.groups.keys())

    batch_size = 100
    num_batches = (len(all_app_ids) + batch_size - 1) // batch_size

    for batch_idx in tqdm(range(num_batches), desc="Processing application_id batches"):
        batch_app_ids = all_app_ids[batch_idx*batch_size : (batch_idx+1)*batch_size]
        edges_batch = []

        for app_id in batch_app_ids:
            group_df = grouped.get_group(app_id)
            if len(group_df) >= 2:
                edges = compute_edges(group_df)
                edges_batch.extend(edges)

        if edges_batch:
            with driver.session() as session:
                session.execute_write(create_allen_relationships, edges_batch)
    
# Ce qui se lance quand on run les fichiers un par un
if __name__ == '__main__':
    driver = connect_to_db(db_loc='local')
    
    # Récup tous les events
    with driver.session() as session:
        df_events = session.execute_read(fetch_events)

    df_events["timestamp"] = pd.to_datetime(df_events["timestamp"])
    print(f"Nombre total d'events récupérés: {len(df_events)}")

    grouped = df_events.groupby("application_id")
    all_app_ids = list(grouped.groups.keys())

    batch_size = 100
    num_batches = (len(all_app_ids) + batch_size - 1) // batch_size

    for batch_idx in tqdm(range(num_batches), desc="Processing application_id batches"):
        batch_app_ids = all_app_ids[batch_idx*batch_size : (batch_idx+1)*batch_size]
        edges_batch = []

        for app_id in batch_app_ids:
            group_df = grouped.get_group(app_id)
            if len(group_df) >= 2:
                edges = compute_edges(group_df)
                edges_batch.extend(edges)

        if edges_batch:
            with driver.session() as session:
                session.execute_write(create_allen_relationships, edges_batch)