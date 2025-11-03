

from config import collection

def save_to_mongodb(docs):
    """Save or update multiple records in MongoDB"""
    for doc in docs:
        collection.update_one(
            {"_id": doc["_id"]},
            {"$set": doc},
            upsert=True
        )
    print(f"✅ Inserted/Updated {len(docs)} records")
