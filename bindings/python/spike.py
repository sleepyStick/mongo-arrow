import random
import time
from datetime import datetime, timezone

import pyarrow as pa
from pymongo import MongoClient

from pymongoarrow.api import Schema, find_arrow_all, find_arrow_all_multiprocesses,find_arrow_all_threading

# Populate the collection with a large dataset
# NUM_DOCUMENTS = 10_000_000
NUM_DOCUMENTS = 50_000

VEC_LEN = 512          # 2K elements
LABELS_LEN = 16
MATRIX_DIM = 64         # 64x64 = 4096 elements

def make_heavy_doc(i: int) -> dict:
    return {
        "id": i,
        "vec_float": [random.random() for _ in range(VEC_LEN)],
        "vec_int": [random.randint(0, 1_000_000) for _ in range(VEC_LEN)],
        "nested_matrix": [
            [random.random() for _ in range(MATRIX_DIM)]
            for _ in range(MATRIX_DIM)
        ],
        "labels": [random.randint(0, 10_000) for _ in range(LABELS_LEN)],
        "timestamp": datetime.now(timezone.utc),
    }

def make_simple_doc(i: int) -> dict:
    return {
        "measures": [random.randint(1, 100) for j in range(10)],
        "num": i
    }


def main():
    random.seed(0)
    """
    Test case to create a MongoDB collection, populate it with a large dataset,
    and measure the performance of find_arrow_all while validating the exported data.
    """
    # Connect to MongoDB
    client = MongoClient()
    db = client["test_database"]
    collection_name = "large_dataset_collection"
    collection = db[collection_name]

    # Drop the collection if it already exists
    db.drop_collection(collection_name)

    print(f"Inserting {NUM_DOCUMENTS} documents into the collection '{collection_name}'...")
    data = [make_simple_doc(i) for i in range(NUM_DOCUMENTS)]
    collection.insert_many(data)
    print(f"Inserted {NUM_DOCUMENTS} documents into the collection.")

    # Define the schema for the Arrow table
    schema = Schema({"measure": pa.list_(pa.int32()), "num": int})
    # schema = Schema({
    #     "id": int,
    #     "vec_float": pa.list_(pa.float64()),
    #     "vec_int": pa.list_(pa.int64()),
    #     "nested_matrix": pa.list_(pa.list_(pa.float64())),
    #     "labels": pa.list_(pa.int32()),
    #     "timestamp": pa.timestamp("ms"),
    # })

    print("-----SINGLE THREADED-----")
    arrow_table = find_arrow_all(collection, {}, schema=schema)

    print("-----MULTIPROCESS-----")
    arrow_table_multiprocess = find_arrow_all_multiprocesses(collection, {},
                                                             schema=schema)

    print("-----MULTITHREAD-----")
    arrow_table_multithread = find_arrow_all_threading(collection, {},
                                                             schema=schema)



    print("-----VALIDATION-----")
    # Perform some sanity check validations
    assert isinstance(arrow_table, pa.Table), "The result is not a PyArrow table."
    assert isinstance(arrow_table_multiprocess,
                      pa.Table), "The result is not a PyArrow table."
    assert isinstance(arrow_table_multithread,
                      pa.Table), "The result is not a PyArrow table."
    assert arrow_table.schema.equals(
        arrow_table_multiprocess.schema
    ), "Schema does not match the expected structure."
    assert arrow_table.schema.equals(
        arrow_table_multithread.schema
    ), "Schema does not match the expected structure."
    print("Validation complete: Data correctly exported.")

    # Clean up after test
    db.drop_collection(collection_name)
    print("Test completed and test collection dropped.")


# RUNNING THE TEST CASE
if __name__ == "__main__":
    main()