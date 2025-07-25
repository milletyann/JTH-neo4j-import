from neo4j import GraphDatabase
import pandas as pd
import math
from tqdm import tqdm
from itertools import combinations
from eventify_df import eventify
from credentials import read_instance_credentials

def create_cand_jobs_nodes(tx, events):
    # Créer tous les candidates
    unique_candidates = list(events["candidate_id"].dropna().unique())
    if unique_candidates:
        tx.run("""
            UNWIND $candidates AS candidate_id
            MERGE (:Candidate {candidate_id: candidate_id})
        """, candidates=unique_candidates)

        print(f"Created {len(unique_candidates)} Candidates")

    # Créer tous les jobs
    unique_jobs = list(events["job_id"].dropna().unique())
    if unique_jobs:
        tx.run("""
            UNWIND $jobs AS job_id
            MERGE (:Job {job_id: job_id})
        """, jobs=unique_jobs)

        print(f"Created {len(unique_jobs)} Jobs")




def create_graph(tx, events):
    # Créer tous les Events et toutes les relations
    events_dicts = events.to_dict("records")
    tx.run("""
        UNWIND $batch AS row
        MERGE (e:Event {
            event_name: row.event_name,
            application_id: row.application_id
        })
        SET e.timestamp = row.timestamp,
            e.candidate_id = row.candidate_id,
            e.job_id = row.job_id

        WITH e, row
        MATCH (c:Candidate {candidate_id: row.candidate_id})
        MERGE (c)-[:HAS_EVENT]->(e)

        WITH e, row
        MATCH (j:Job {job_id: row.job_id})
        MERGE (j)-[:HAS_EVENT]->(e)
    """, batch=events_dicts)
    print(f"Created {len(events)} Events and relations")


if __name__ == "__main__":
    print("Connecting to AuraDB instance...")
    creds = read_instance_credentials()
    uri = creds["NEO4J_URI"]
    username = creds["NEO4J_USERNAME"]
    password = creds["NEO4J_PASSWORD"]

    driver = GraphDatabase.driver(
        uri,
        auth=(username, password),
        connection_timeout=120
    )

    df_events = eventify()

    batch_size = 1000
    num_batches = math.ceil(len(df_events) / batch_size)

    with driver.session() as session:
        # Créer tous les candidates et jobs une seule fois
        session.execute_write(create_cand_jobs_nodes, df_events)

        for i in tqdm(range(num_batches), desc="Processing event batches"):
            start = i * batch_size
            end = min((i+1)*batch_size, len(df_events))
            batch_df = df_events.iloc[start:end]
            session.execute_write(create_graph, batch_df)