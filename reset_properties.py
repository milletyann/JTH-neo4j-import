import math
from tqdm import tqdm
from connect_to_db import connect_to_db

BATCH_SIZE = 500

def get_all_ids(tx, label, id_key):
    result = tx.run(f"""
        MATCH (n:{label})
        RETURN n.{id_key} AS id
    """)
    return [record["id"] for record in result]

def clear_candidate_properties(tx):
    tx.run("""
        MATCH (c:Candidate)
        WITH c, [k IN keys(c) WHERE k <> 'candidate_id'] AS props
        FOREACH (key IN props | 
            REMOVE c[key]
        )
    """)

def clear_job_properties(tx):
    tx.run("""
        MATCH (j:Job)
        WITH j, [k IN keys(j) WHERE k <> 'job_id'] AS props
        FOREACH (key IN props | 
            REMOVE j[key]
        )
    """)

def clear_properties_batch(tx, label, id_key, ids_batch):
    tx.run(f"""
        UNWIND $ids AS id
        MATCH (n:{label} {{ {id_key}: id }})
        WITH n, [k IN keys(n) WHERE k <> '{id_key}'] AS props
        FOREACH (key IN props | REMOVE n[key])
    """, ids=ids_batch)

def clear_node_properties(driver, label, id_key):
    with driver.session() as session:
        all_ids = session.execute_read(get_all_ids, label, id_key)

    # Découper en batchs
    num_batches = math.ceil(len(all_ids) / BATCH_SIZE)
    for i in tqdm(range(num_batches), desc=f"Clearing properties for {label}"):
        batch_ids = all_ids[i * BATCH_SIZE : (i+1) * BATCH_SIZE]
        with driver.session() as session:
            session.execute_write(clear_properties_batch, label, id_key, batch_ids)

def reset_properties(driver):
    clear_node_properties(driver, "Candidate", "candidate_id")
    clear_node_properties(driver, "Job", "job_id")
    

if __name__ == "__main__":
    driver = connect_to_db(db_loc='local')

    clear_node_properties(driver, "Candidate", "candidate_id")
    clear_node_properties(driver, "Job", "job_id")

    driver.close()