import csv

def load_sources(path: str):
    sources = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sources.append(row)
    return sources
