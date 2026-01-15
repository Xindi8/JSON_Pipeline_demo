import json
from datetime import datetime
from pymongo import MongoClient

def parse_published(value):
    # Accept ISO like "2015-01-02T03:04:05Z"
    if not isinstance(value, str):
        return value
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return value  # keep original if parse fails

def ingest_jsonl(mongo_uri, db_name, collection_name, jsonl_path, batch_size=1000, drop_first=True):
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=4000)
    client.admin.command("ping")

    db = client[db_name]
    col = db[collection_name]

    if drop_first:
        col.drop()

    batch = []
    inserted = 0

    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if "published" in obj:
                obj["published"] = parse_published(obj["published"])
            batch.append(obj)

            if len(batch) >= batch_size:
                col.insert_many(batch)
                inserted += len(batch)
                batch = []

    if batch:
        col.insert_many(batch)
        inserted += len(batch)

    client.close()
    return inserted
