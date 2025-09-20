import numpy as np
import io
from sentence_transformers import SentenceTransformer
import mysql.connector
from multiprocessing import Pool, Manager

model = None
scraped_db_config = None
vectors_db_config = None
vectors_table_name = None

def initialize_database(db_config, table_name):
    conn = None
    try:
        temp_config = db_config.copy()
        db_name = temp_config.pop('database')
        conn = mysql.connector.connect(**temp_config)
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`;")
        conn.commit()
        conn.close()
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            site VARCHAR(255) PRIMARY KEY,
            content LONGBLOB
        );
        """
        cursor.execute(create_table_sql)
        conn.commit()
        print(f"Database '{db_name}' and table '{table_name}' are ready.")
        return conn
    except mysql.connector.Error as e:
        print(f"Error initializing database: {e}")
        if conn and conn.is_connected():
            conn.close()
        return None

def get_unvectorized_data(scraped_db_config, vectors_db_config):
    scraped_conn = None
    try:
        scraped_conn = mysql.connector.connect(**scraped_db_config)
        scraped_cursor = scraped_conn.cursor()
        query_sql = """
        SELECT T1.site, T1.content
        FROM html_data AS T1
        LEFT JOIN vectors.vectors_data AS T2
        ON T1.site = T2.site
        WHERE T2.site IS NULL;
        """
        scraped_cursor.execute(query_sql)
        data_to_vectorize = scraped_cursor.fetchall()
        print(f"Found {len(data_to_vectorize)} new sites to vectorize.")
        return data_to_vectorize
    except mysql.connector.Error as e:
        print(f"Error retrieving data: {e}")
        return []
    finally:
        if scraped_conn and scraped_conn.is_connected():
            scraped_conn.close()

def process_site(site_data, vectors_db_config):
    conn = None
    site, content = None, None
    try:
        global model
        if model is None:
            model = SentenceTransformer('sentence-transformers/all-MiniLM-L6-v2')
        site, content = site_data
        conn = mysql.connector.connect(**vectors_db_config)
        cursor = conn.cursor()
        embedding = model.encode(content, convert_to_numpy=True)
        buffer = io.BytesIO()
        np.save(buffer, embedding)
        vector_data = buffer.getvalue()
        insert_sql = f"""
        INSERT INTO vectors_data (site, content) VALUES (%s, %s);
        """
        cursor.execute(insert_sql, (site, vector_data))
        conn.commit()
        print(f"Successfully vectorized and saved: {site}")

    except Exception as e:
        if site:
            print(f"Could not vectorize site {site}: {e}")
        else:
            print(f"An error occurred: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()

def main():
    global scraped_db_config, vectors_db_config, vectors_table_name
    scraped_db_config = {
        "user": "root",
        "password": "1234",
        "host": "127.0.0.1",
        "database": "scraped"
    }
    vectors_db_config = scraped_db_config.copy()
    vectors_db_config["database"] = "vectors"
    vectors_table_name = "vectors_data"
    vectors_conn = initialize_database(vectors_db_config, vectors_table_name)
    if not vectors_conn:
        return
    data_to_vectorize = get_unvectorized_data(scraped_db_config, vectors_db_config)
    if not data_to_vectorize:
        print("No new data to vectorize. Exiting.")
        if vectors_conn and vectors_conn.is_connected():
            vectors_conn.close()
        return
    if vectors_conn and vectors_conn.is_connected():
        vectors_conn.close()
    print("Starting multiprocessing for vectorization...")
    with Pool() as pool:
        pool.starmap(process_site, [(data, vectors_db_config) for data in data_to_vectorize])
    print("Vectorization process complete.")

if __name__ == '__main__':
    main()