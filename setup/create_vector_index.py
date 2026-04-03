"""
One-time Setup: Create Atlas Vector Search Index

Run this once after creating your MongoDB Atlas cluster.
It creates the vector search index on the `embedding` field
of the `loan_applications` collection.

Usage:
    python setup/create_vector_index.py

Requirements:
    - MONGODB_URI and MONGODB_DATABASE in your .env
    - Atlas cluster (M10+ or serverless — free tier M0 supports Vector Search)
    - The pymongo driver (not motor — this is a sync setup script)
"""

import os
import time

from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.operations import SearchIndexModel

load_dotenv()

INDEX_NAME = "loan_embedding_index"
EMBEDDING_DIMENSIONS = 1024  # voyage-4-large output dimensions
SIMILARITY = "cosine"


def main() -> None:
    uri = os.environ["MONGODB_URI"]
    db_name = os.environ.get("MONGODB_DATABASE", "loan_processing")

    print(f"Connecting to MongoDB Atlas...")
    client = MongoClient(uri)
    db = client[db_name]
    collection = db["loan_applications"]

    # Check if index already exists
    existing = list(collection.list_search_indexes())
    if any(idx["name"] == INDEX_NAME for idx in existing):
        print(f"Vector Search index '{INDEX_NAME}' already exists — nothing to do.")
        return

    # Atlas requires the collection to exist before a search index can be created.
    # Create it now if it doesn't exist yet (the seed script will populate it).
    if "loan_applications" not in db.list_collection_names():
        print("Collection 'loan_applications' not found — creating it...")
        db.create_collection("loan_applications")
        print("Collection created ✓")

    print(f"Creating Atlas Vector Search index '{INDEX_NAME}'...")
    print(f"  Collection : {db_name}.loan_applications")
    print(f"  Field      : embedding")
    print(f"  Dimensions : {EMBEDDING_DIMENSIONS}")
    print(f"  Similarity : {SIMILARITY}")
    print()

    index_model = SearchIndexModel(
        definition={
            "fields": [
                {
                    "type": "vector",
                    "path": "embedding",
                    "numDimensions": EMBEDDING_DIMENSIONS,
                    "similarity": SIMILARITY,
                },
                # Filter field: only search within decided applications
                {
                    "type": "filter",
                    "path": "decision.outcome",
                },
                {
                    "type": "filter",
                    "path": "_id",
                },
            ]
        },
        name=INDEX_NAME,
        type="vectorSearch",
    )

    collection.create_search_index(model=index_model)

    # Atlas indexes build asynchronously — poll until ready
    print("Waiting for index to become active", end="", flush=True)
    for _ in range(60):  # up to ~5 minutes
        time.sleep(5)
        indexes = {idx["name"]: idx for idx in collection.list_search_indexes()}
        if INDEX_NAME in indexes:
            status = indexes[INDEX_NAME].get("status", "UNKNOWN")
            if status == "READY":
                print(f"\n\nIndex '{INDEX_NAME}' is READY ✓")
                print("You can now run the seed script: python seed/seed_historical_loans.py")
                return
            print(".", end="", flush=True)

    print(f"\n\nIndex created but not yet READY — check Atlas UI for status.")
    print("It may take a few more minutes. Re-run this script to check again.")


if __name__ == "__main__":
    main()
