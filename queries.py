import re
from datetime import datetime, timedelta
from collections import Counter

WORD_RE = re.compile(r"[A-Za-z0-9_-]+")

def top_words_by_media_type(col, media_type, top_k=5):
    q = {"media_type": {"$regex": f"^{media_type}$", "$options": "i"}}
    cur = col.find(q, {"title": 1, "content": 1, "_id": 0})

    c = Counter()
    for doc in cur:
        for field in ("title", "content"):
            if isinstance(doc.get(field), str):
                c.update(WORD_RE.findall(doc[field].lower()))

    if not c:
        return []
    # include ties at kth
    items = c.most_common()
    cutoff = items[top_k - 1][1] if len(items) >= top_k else items[-1][1]
    return [(w, cnt) for w, cnt in items if cnt >= cutoff]

def count_diff_by_day(col, day_str):
    day = datetime.strptime(day_str, "%Y-%m-%d")
    start = day
    end = day + timedelta(days=1)

    def cnt(media):
        q = {
            "media_type": {"$regex": f"^{media}$", "$options": "i"},
            "published": {"$gte": start, "$lt": end},
        }
        return col.count_documents(q)

    return {"date": day_str, "news": cnt("news"), "blog": cnt("blog")}

def top_sources_in_year(col, year=2015, top_k=5):
    start = datetime(year, 1, 1)
    end = datetime(year + 1, 1, 1)

    pipeline = [
        {"$match": {"media_type": {"$regex": "^news$", "$options": "i"}, "published": {"$gte": start, "$lt": end}}},
        {"$group": {"_id": "$source", "cnt": {"$sum": 1}}},
        {"$sort": {"cnt": -1, "_id": 1}},
    ]
    rows = list(col.aggregate(pipeline))
    if not rows:
        return []
    cutoff = rows[top_k - 1]["cnt"] if len(rows) >= top_k else rows[-1]["cnt"]
    return [{"source": r["_id"], "count": r["cnt"]} for r in rows if r["cnt"] >= cutoff]

def most_recent_by_source(col, source, limit=5):
    q = {"source": {"$regex": f"^{source}$", "$options": "i"}}
    pipeline = [
        {"$match": q},
        {"$sort": {"published": -1}},
        {"$limit": limit},
        {"$project": {"_id": 0, "title": 1, "published": 1, "source": 1}},
    ]
    return list(col.aggregate(pipeline))
