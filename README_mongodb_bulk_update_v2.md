# MongoDB Bulk Update Script (V2)

## Goal

This script performs high-performance bulk updates on MongoDB collections by processing documents in parallel batches. It uses a producer-consumer pattern with a thread-safe queue to efficiently update large collections while avoiding batch overlaps and maximizing throughput. The script is designed to handle millions of documents by splitting the work into configurable batch sizes and processing them concurrently across multiple worker threads.

## Architecture: Queue-Based Parallelization

The script uses a **producer-consumer pattern** with a thread-safe queue to parallelize MongoDB updates:

- **Single Reader Worker (Producer)**: One dedicated thread fetches batches of document IDs sequentially from MongoDB, using cursor-based pagination (`_id > last_id`) to ensure no documents are skipped or duplicated. Each batch is placed into a shared queue.

- **Multiple Writer Workers (Consumers)**: Multiple worker threads (configurable via `MAX_WORKERS`) pull batches from the queue and perform `update_many` operations in parallel. This allows multiple batches to be processed simultaneously.

- **Why Single Reader + Multiple Writers?**: Using a single reader ensures batches are fetched in order without overlaps. If multiple readers were used, they could fetch overlapping batches (e.g., both reading documents 1-2000), causing duplicate work or missed documents. The single reader maintains sequential batch boundaries while multiple writers process those batches concurrently, maximizing parallelism without risking data integrity.

## Configuration

Before running the script, you must update the following configuration:

### 1. MongoDB Connection Settings (Lines 8-10)

```python
MONGO_URI = "mongodb+srv://usr:pwd@your-cluster.mongodb.net/?w=1"
DB_NAME = "db_name"
COLLECTION_NAME = "collection_name"
```

- **MONGO_URI**: Replace with your MongoDB connection string (include credentials)
- **DB_NAME**: Replace with your database name
- **COLLECTION_NAME**: Replace with your target collection name

### 2. Update Operation (Lines 95-96)

```python
[{"$set": {"new_field": {"$concat": ["$author", "_", "$title"]}}}]
```

- **Field Name**: Change `"new_field"` to your desired field name
- **Concatenation Logic**: Modify the `$concat` array to match your requirements:
  - Current: `["$author", "_", "$title"]` - concatenates author and title with underscore
  - Example: `["$author", " ", "$title"]` - concatenates with space
  - Example: `["$field1", "-", "$field2", "-", "$field3"]` - multiple fields with separators

## Performance Tuning

- **MAX_WORKERS**: Increase to utilize more IOPS (try 16, 24, or 32 depending on your MongoDB cluster limits)
- **BATCH_SIZE**: Larger batches reduce network round-trips but increase memory usage (try 2000, 5000, or 10000)
- **MAX_QUEUE_SIZE**: Larger queue allows better pipelining between reader and writers (should be >= MAX_WORKERS)

## Usage

```bash
python3 mongodb_bulk_update_v2.py
```

The script will display progress updates every minute and show final statistics upon completion.

