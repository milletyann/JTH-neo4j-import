from neo4j import GraphDatabase
import pandas as pd
import numpy as np
from tqdm import tqdm
from credentials import read_instance_credentials

def preprocess_candidates(df):
    df = df.copy()

    df["skills"] = df["skills"].fillna("").apply(
        lambda x: [s.strip() for s in x.split(";") if s.strip()] if x else []
    )
    df["job_category"] = df["job_category"].fillna("").apply(
        lambda x: [s.strip() for s in x.split(";") if s.strip()] if x else []
    )
    df["contract_type"] = df["contract_type"].fillna("").apply(
        lambda x: [s.strip() for s in x.split(";") if s.strip()] if x else []
    )
    df["create_date"] = pd.to_datetime(df["create_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["zipcode"] = df["zipcode"].apply(
        lambda x: int(x) if pd.notnull(x) and str(x).strip() != "" else None
    )
    df["actual_salary"] = df["actual_salary"].replace({np.nan: None})
    df["actual_daily_salary"] = df["actual_daily_salary"].replace({np.nan: None})
    df["applicant"] = df["applicant"].fillna(False).astype(bool)

    return df
    
def preprocess_jobs(df):
    df = df.copy()

    df["skills"] = df["skills"].fillna("").apply(
        lambda x: [s.strip() for s in x.split(";") if s.strip()] if x else []
    )
    df["create_date"] = pd.to_datetime(df["create_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["zipcode"] = df["zipcode"].apply(
        lambda x: int(x) if pd.notnull(x) and str(x).strip() != "" else None
    )
    df["salary"] = df["salary"].replace({np.nan: None})
    df["daily_rate"] = df["daily_rate"].replace({np.nan: None})
    df["useful_job"] = df["useful_job"].fillna(False).astype(bool)

    return df

def update_candidates(tx, df_candidates):
    candidates_dicts = df_candidates.to_dict("records")
    tx.run("""
        UNWIND $rows AS row
        MERGE (c:Candidate {candidate_id: row.candidate_id})
        SET c += row
    """, rows=candidates_dicts)

def update_jobs(tx, df_jobs):
    jobs_dicts = df_jobs.to_dict("records")
    tx.run("""
        UNWIND $rows AS row
        MERGE (j:Job {job_id: row.job_id})
        SET j += row
    """, rows=jobs_dicts)

        
def run_batch_update_cand(driver, df_candidates, batch_size=500):
    total_rows = len(df_candidates)
    num_batches = (total_rows + batch_size - 1) // batch_size

    for i in tqdm(range(0, total_rows, batch_size), total=num_batches, desc="Updating candidates properties"):
        df_batch = df_candidates.iloc[i:i+batch_size]
        with driver.session() as session:
            session.execute_write(update_candidates, df_batch)

def run_batch_update_jobs(driver, df_jobs, batch_size=500):
    total_rows = len(df_jobs)
    num_batches = (total_rows + batch_size - 1) // batch_size

    for i in tqdm(range(0, total_rows, batch_size), total=num_batches, desc="Updating jobs properties"):
        df_batch = df_jobs.iloc[i:i+batch_size]
        with driver.session() as session:
            session.execute_write(update_jobs, df_batch)

if __name__ == '__main__':
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
    df_candidates, df_jobs = preprocess_candidates(pd.read_csv("candidates.csv")), preprocess_jobs(pd.read_csv("jobs.csv"))
    
    run_batch_update_cand(driver, df_candidates, batch_size=500)
    run_batch_update_jobs(driver, df_jobs, batch_size=500)

    driver.close()
