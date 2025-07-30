from connect_to_db import connect_to_db

def delete_graph(driver, batch_size=5000):
    with driver.session() as session:
        while True:
            result = session.run("MATCH (n) WITH n LIMIT $batch_size DETACH DELETE n RETURN count(n) AS deleted", batch_size=batch_size)
            deleted_count = result.single()["deleted"]

            if deleted_count == 0:
                break


if __name__ == '__main__':
    driver = connect_to_db(db_loc='local')
    
    delete_graph(driver)

    driver.close()