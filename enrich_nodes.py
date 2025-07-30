import pandas as pd
import numpy as np
from tqdm import tqdm
from connect_to_db import connect_to_db

def preprocess_candidates(df):
    df = df.copy()

    df["skills"] = df["skills"].fillna("").apply(
        lambda x: [s.strip() for s in x.split(";") if s.strip()] if x else []
    )
    df["job_category"] = df["job_category"].fillna("").apply(
        lambda x: [s.strip() for s in x.split(";") if s.strip()] if x else []
    )
    df["create_date"] = pd.to_datetime(df["create_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["zipcode"] = df["zipcode"].apply(
        lambda x: int(x) if pd.notnull(x) and str(x).strip() != "" else None
    )
    df["actual_salary"] = df["actual_salary"].replace({np.nan: None})
    df["actual_daily_salary"] = df["actual_daily_salary"].replace({np.nan: None})

    return df
    
def preprocess_jobs(df):
    df = df.copy()

    df["skills"] = df["skills"].fillna("").apply(
        lambda x: [s.strip() for s in x.split(";") if s.strip()] if x else []
    )
    df["create_date"] = pd.to_datetime(df["create_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    #df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df["zipcode"] = df["zipcode"].apply(
        lambda x: int(x) if pd.notnull(x) and str(x).strip() != "" else None
    )
    df["salary"] = df["salary"].replace({np.nan: None})
    df["daily_rate"] = df["daily_rate"].replace({np.nan: None})

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

# La fonction à utiliser dans run.py
def enrich_nodes(driver):
    df_candidates, df_jobs = preprocess_candidates(pd.read_csv("JTH/candidates.csv")), preprocess_jobs(pd.read_csv("JTH/jobs.csv"))
    
    run_batch_update_cand(driver, df_candidates, batch_size=500)
    run_batch_update_jobs(driver, df_jobs, batch_size=500)
    
# Ce qui se lance quand on run les fichiers un par un
if __name__ == '__main__':
    driver = connect_to_db(db_loc='local')
    df_candidates, df_jobs = preprocess_candidates(pd.read_csv("JTH/candidates.csv")), preprocess_jobs(pd.read_csv("JTH/jobs.csv"))
    
    run_batch_update_cand(driver, df_candidates, batch_size=500)
    run_batch_update_jobs(driver, df_jobs, batch_size=500)

    driver.close()
