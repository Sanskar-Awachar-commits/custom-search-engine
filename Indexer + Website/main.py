import numpy as np
import io
import json
from flask import Flask, request, render_template, jsonify
from sentence_transformers import SentenceTransformer
import mysql.connector
from scipy.spatial.distance import cosine


db_config = {
    "user": "root",
    "password": "1234",
    "host": "127.0.0.1",
    "database": "vectors",
}


vectors_table_name = "vectors_data"


app = Flask(__name__)


print("Loading Sentence-Transformer model...")
try:
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    print("Model loaded successfully.")
except Exception as e:
    print(f"Failed to load model: {e}")
    print(
        "Please ensure you have `sentence-transformers` installed (`pip install -U sentence-transformers`)."
    )
    model = None


def get_all_vectors():
    conn = None
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        query_sql = f"SELECT site, content FROM {vectors_table_name};"
        cursor.execute(query_sql)

        results = []
        for site, content_blob in cursor:

            try:
                buffer = io.BytesIO(content_blob)
                vector = np.load(buffer)
                results.append({"site": site, "vector": vector})
            except Exception as e:
                print(f"Error loading vector for site {site}: {e}")

        print(f"Retrieved {len(results)} vectors from the database.")
        return results
    except mysql.connector.Error as e:
        print(f"Error retrieving vectors from database: {e}")
        return []
    finally:
        if conn and conn.is_connected():
            conn.close()


def find_top_n_matches(query_vector, all_vectors, n=50):
    similarities = []

    for doc in all_vectors:
        doc_vector = doc["vector"]
        similarity = 1 - cosine(query_vector, doc_vector)

        similarities.append({"site": doc["site"], "similarity": float(similarity)})

    similarities.sort(key=lambda x: x["similarity"], reverse=True)

    return similarities[:n]


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/search", methods=["POST"])
def search_api():
    if model is None:
        return jsonify({"error": "Model not loaded"}), 500

    try:
        data = request.get_json()
        query = data.get("query")

        if not query:
            return jsonify({"error": "No query provided"}), 400

        query_vector = model.encode(query, convert_to_numpy=True)

        all_doc_vectors = get_all_vectors()

        if not all_doc_vectors:
            return jsonify({"results": []})

        top_results = find_top_n_matches(query_vector, all_doc_vectors, n=50)

        return jsonify({"results": top_results})

    except Exception as e:
        print(f"An error occurred during search: {e}")
        return jsonify({"error": "Internal server error"}), 500


if __name__ == "__main__":

    app.run(debug=True)
