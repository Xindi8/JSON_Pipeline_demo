# JOSN_Pipeline_demo

Author: Xindi Li

## What this program does
This project ingests a **JSONL (JSON Lines)** dataset into **MongoDB** and runs simple analytics queries from the command line.
It is a small end-to-end demo of a real-world data workflow: **load → clean → query → report**.

## Skills demonstrated
- Python scripting for data pipelines
- JSON/JSONL parsing and streaming file processing
- Batch insertion into MongoDB (PyMongo)
- Basic data cleaning (ISO `published` datetime parsing)
- Writing MongoDB queries + aggregation pipelines
- Building a CLI tool using `argparse`

## How to run

## 1) Install dependency
```bash
pip install pymongo
```
### 2) Start MongoDB (local)
```bash
Default connection: mongodb://localhost:27017
```
### 3) Ingest JSONL into MongoDB
```bash
python main.py ingest --jsonl sample_articles.jsonl --batch 1000 --drop
```
### 4) Run sample queries
Top words by media type
```bash
python main.py query --mode top_words --media news
```
Count news vs blog on a given day

```bash
python main.py query --mode day_diff --date 2015-01-01
```
Top news sources in a year

```bash
python main.py query --mode top_sources --year 2015
```
Most recent articles from a source

```bash
python main.py query --mode recent --source "Example News"
```
## Notes
Default database settings are defined in config.py (db: demo_db, collection: articles).

