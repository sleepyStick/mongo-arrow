import random
import time
from datetime import datetime, timezone

import psutil
import os

import pyarrow as pa
from pymongo import MongoClient

from pymongoarrow.api import Schema, find_arrow_all, find_arrow_all_multiprocesses

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

def measure(func, args, **kwargs):
    # current_process = psutil.Process(os.getpid())
    # current_process.cpu_percent(interval=None)
    ### Start measuring
    start = time.monotonic()
    process_start = time.process_time()

    # parent = psutil.Process()
    # def total_cpu_time():
    #     procs = [parent] + parent.children(recursive=True)
    #     total = 0.0
    #     for p in procs:
    #         try:
    #             t = p.cpu_times()
    #         except psutil.NoSuchProcess:
    #             continue
    #         total += t.user + t.system
    #     return total
    #
    # start_cpu = total_cpu_time()

    # Use find_arrow_all to export data into an Arrow table
    ret_val = func(*args, **kwargs)

    # Calculate elapsed time and cpu usage
    # cpu_usage_percent = current_process.cpu_percent(interval=1)
    end = time.monotonic()
    process_end = time.process_time()
    # end_cpu = total_cpu_time()
    #
    # duration = end - start
    # total_cpu = (end_cpu - start_cpu)
    #
    # # Normalize vs number of logical CPUs to get average utilization %
    # n_cpus = psutil.cpu_count(logical=True) or 1
    # cpu_percent = (total_cpu / duration) * 100
    #
    # print(f"CPU seconds (parent+children): {total_cpu:.3f}s")
    # print(f"Wall time: {duration:.3f}s")
    # print(f"Total CPU utilization: {cpu_percent:.1f}%")
    #
    # print(f"{duration=:.3f}s, total_cpu={total_cpu:.3f}s")
    # print(f"Average CPU utilization: {cpu_percent:.1f}% with {n_cpus} cores")

    #
    # print(end - start)
    # print(f"{duration=}")
    duration = end - start
    process_usage = int((process_end - process_start) / duration * 100)
    print(
        f"Time taken for find_arrow_all with {NUM_DOCUMENTS} documents: {duration:.3f} seconds"
    )
    print(f"CPU Usage %: {process_usage}")
    # print(f"{cpu_usage_percent=}")
    return ret_val


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
    # data = [{"measures": [random.randint(1, 100) for j in range(10)],
    #          "timestamp": time.time(),
    #          "num": i}
    #         for i in range(NUM_DOCUMENTS)]
    data = [make_heavy_doc(i) for i in range(NUM_DOCUMENTS)]
    collection.insert_many(data)
    print(f"Inserted {NUM_DOCUMENTS} documents into the collection.")

    # Define a query predicate to filter documents with measure > 40
    # query_predicate = {"measure": {"$gt": 50}}

    # Define the schema for the Arrow table
    # schema = Schema({"measure": pa.list_(pa.int32()), "num": int})
    schema = Schema({
        "id": int,
        "vec_float": pa.list_(pa.float64()),
        "vec_int": pa.list_(pa.int64()),
        "nested_matrix": pa.list_(pa.list_(pa.float64())),
        "labels": pa.list_(pa.int32()),
        "timestamp": pa.timestamp("ms"),
    })

    print("-----MULTIPROCESS-----")
    arrow_table_multiprocess = measure(find_arrow_all_multiprocesses,
                                       (collection, {}), schema=schema)

    print("-----BEFORE-----")
    arrow_table = measure(find_arrow_all, (collection, {}), schema=schema)



    print("-----VALIDATION-----")
    # Validate the exported data
    print("Arrow Table Content (first 5 rows):")
    print(arrow_table.slice(0, 5))  # Print first 5 rows for brevity
    print("Arrow Table Multiprocess Content (first 5 rows):")
    print(arrow_table_multiprocess.slice(0, 5))  # Print first 5 rows for brevity

    # Perform additional validations to ensure data correctness
    assert isinstance(arrow_table, pa.Table), "The result is not a PyArrow table."
    assert isinstance(arrow_table_multiprocess,
                      pa.Table), "The result is not a PyArrow table."
    assert arrow_table.schema.equals(
        arrow_table_multiprocess.schema
    ), "Schema does not match the expected structure."
    print("Validation complete: Data correctly exported.")

    # Clean up after test
    db.drop_collection(collection_name)
    print("Test completed and test collection dropped.")


# RUNNING THE TEST CASE
# Ensure you replace <your_connection_string> with the actual connection string to your MongoDB instance.
if __name__ == "__main__":
    main()