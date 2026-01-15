import argparse
from pymongo import MongoClient
from config import default_config
from ingest_jsonl import ingest_jsonl
import queries

def get_col(uri, db_name, collection):
    client = MongoClient(uri, serverSelectionTimeoutMS=4000)
    client.admin.command("ping")
    return client, client[db_name][collection]

def main():
    cfg = default_config()

    p = argparse.ArgumentParser()
    p.add_argument("--uri", default=cfg["mongo_uri"])
    p.add_argument("--db", default=cfg["db_name"])
    p.add_argument("--col", default=cfg["collection"])
    sub = p.add_subparsers(dest="cmd", required=True)

    s_ing = sub.add_parser("ingest")
    s_ing.add_argument("--jsonl", required=True)
    s_ing.add_argument("--batch", type=int, default=cfg["batch_size"])
    s_ing.add_argument("--drop", action="store_true")

    s_q = sub.add_parser("query")
    s_q.add_argument("--mode", choices=["top_words", "day_diff", "top_sources", "recent"], required=True)
    s_q.add_argument("--media", default="news")
    s_q.add_argument("--date", default="2015-01-01")
    s_q.add_argument("--year", type=int, default=2015)
    s_q.add_argument("--source", default="Example News")

    args = p.parse_args()

    if args.cmd == "ingest":
        n = ingest_jsonl(args.uri, args.db, args.col, args.jsonl, batch_size=args.batch, drop_first=args.drop)
        print(f"[OK] Inserted {n} documents.")
        return

    client, col = get_col(args.uri, args.db, args.col)
    try:
        if args.mode == "top_words":
            print(queries.top_words_by_media_type(col, args.media))
        elif args.mode == "day_diff":
            print(queries.count_diff_by_day(col, args.date))
        elif args.mode == "top_sources":
            print(queries.top_sources_in_year(col, args.year))
        elif args.mode == "recent":
            print(queries.most_recent_by_source(col, args.source))
    finally:
        client.close()

if __name__ == "__main__":
    main()
