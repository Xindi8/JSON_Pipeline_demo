def default_config():
    return {
        "mongo_uri": "mongodb://localhost:27017",
        "db_name": "demo_db",
        "collection": "articles",
        "batch_size": 1000,
    }
