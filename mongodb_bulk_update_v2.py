from pymongo import MongoClient
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue

# --- Configuration ---
MONGO_URI = "mongodb+srv://usr:pwd@your-cluster.mongodb.net/?w=1"
DB_NAME = "db_name"
COLLECTION_NAME = "collection_name"
BATCH_SIZE = 2000           # Set as high as your EC2/mongo can handle (try 2000/5000/10000...)
MAX_QUEUE_SIZE = 24         # Allow for more queuing (tuneable)
MAX_WORKERS = 16            # Set as high as your EC2/mongo can handle (try 8/12/16/24...)
END_TOKEN = object()        # Unique object to signal end-of-data

# --- Shared state for progress ---
num_updated = 0
total_processed = 0
num_updated_lock = threading.Lock()
total_processed_lock = threading.Lock()

# Thread-safe batch queue
batch_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE)
stop_fetching = threading.Event()

# --- Batch Fetching Thread ---
def fetch_batches_worker():
    client = MongoClient(MONGO_URI, tlsAllowInvalidCertificates=True)
    database = client[DB_NAME]
    collection = database[COLLECTION_NAME]
    last_id = None
    batch_num = 0

    try:
        while not stop_fetching.is_set():
            if last_id is None:
                query = {}
            else:
                query = {"_id": {"$gt": last_id}}
            
            docs = list(collection.find(query, {"_id": 1})
                                  .sort("_id", 1)
                                  .limit(BATCH_SIZE))
            
            if not docs:
                # Put sentinel objects for each worker to signal shutdown
                for _ in range(MAX_WORKERS):
                    batch_queue.put(END_TOKEN)
                break

            batch_num += 1

            batch_data = {
                'batch_num': batch_num,
                'docs': docs,
                'last_id': docs[-1]["_id"]
            }
            batch_queue.put(batch_data)
            last_id = docs[-1]["_id"]
    except Exception as e:
        print(f"❌ Fetch worker error: {e}")
    finally:
        client.close()

# --- Batch Update Worker ---
def update_batch_worker():
    client = MongoClient(MONGO_URI, tlsAllowInvalidCertificates=True)
    database = client[DB_NAME]
    collection = database[COLLECTION_NAME]
    updated_local = 0
    processed_local = 0
    
    # Get worker ID for logging
    worker_id = threading.current_thread().name

    while True:
        batch_data = batch_queue.get()
        
        if batch_data is END_TOKEN:
            batch_queue.put(END_TOKEN)  # Pass token along for other workers
            break

        docs = batch_data['docs']
        processed_local += len(docs)

        # Update ALL documents in the batch (force update even if value is same)
        id_list = [doc["_id"] for doc in docs]
        
        # Retry logic for interrupted operations
        max_retries = 3
        for attempt in range(max_retries):
            try:
                result = collection.update_many(
                    {"_id": {"$in": id_list}},
                    [{"$set": {"new_field": {"$concat": ["$author", "_", "$title"]}}}],
                    upsert=False
                )
                # Count all documents processed, not just modified
                updated_local += len(docs)
                print(f"Batch {batch_data['batch_num']}: Processed {len(docs)} docs [Worker: {worker_id}]")
                
                # Update shared counters after each batch
                global num_updated, total_processed
                with num_updated_lock:
                    num_updated += len(docs)
                with total_processed_lock:
                    total_processed += len(docs)
                    
                break  # Success, exit retry loop
            except Exception as e:
                if "InterruptedDueToReplStateChange" in str(e) and attempt < max_retries - 1:
                    print(f"⚠️  Batch {batch_data['batch_num']}: Retry {attempt + 1}/{max_retries} due to cluster interruption [Worker: {worker_id}]")
                    time.sleep(2)  # Wait 2 seconds before retry
                    continue
                else:
                    print(f"❌ Update worker error: {e}")
                    break  # Give up after max retries or non-retryable error

    client.close()

# --- Main Execution ---
print("🚀 Starting V2: Force update ALL documents with new_field (author + title)...")

# Get total document count for progress
client = MongoClient(MONGO_URI, tlsAllowInvalidCertificates=True)
database = client[DB_NAME]
collection = database[COLLECTION_NAME]
total_docs = collection.estimated_document_count()
print(f"📊 Total documents in collection: {total_docs:,}")
print(f"⚠️  V2 will update ALL documents with new_field = author + ' ' + title")
client.close()

# Start fetch worker thread
fetch_thread = threading.Thread(target=fetch_batches_worker)
fetch_thread.start()

# Start update worker threads in the pool
start_time = time.time()
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # Submit each update_batch_worker as a long-lived task
    futures = [executor.submit(update_batch_worker) for _ in range(MAX_WORKERS)]

    # Progress monitoring loop (main thread)
    prev_updated = 0
    while any(future.running() for future in futures):
        time.sleep(60)  # Update progress every 1 minute
        with num_updated_lock:
            updated = num_updated
        with total_processed_lock:
            processed = total_processed

        # Debug: Always show progress, even if processed is 0
        print(f"🔍 Debug: processed={processed}, updated={updated}, total_docs={total_docs}")
        
        if processed == 0:
            print("⏳ Waiting for first batch to complete...")
            continue
        progress = (processed / total_docs) * 100
        elapsed_time = time.time() - start_time
        rate = updated / elapsed_time if elapsed_time > 0 else 0
        print(f"📦 Progress: {min(progress,100):.1f}% - Updated {updated:,} docs so far - Rate: {rate:.0f} docs/sec - Queue size: {batch_queue.qsize()}")

# Wait for fetch worker thread
stop_fetching.set()
fetch_thread.join()

elapsed_time = time.time() - start_time
print(f"\n✅ Total documents updated: {num_updated:,}")
print(f"⏱️  Total time: {elapsed_time:.1f}s ({elapsed_time/60:.2f} minutes)")
print(f"🚀 Average rate: {num_updated/elapsed_time:.0f} docs/sec")
