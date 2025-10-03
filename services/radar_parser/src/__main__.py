from app.config import load_sources
from app.fetch.rssbridge import fetch_rssbridge
from app.parsers.generic_parser import parse_atom
from app.filter.time_filter import filter_last_hours

def main():
    sources = load_sources("config/sources.csv")

    all_items = []
    for src in sources:
        if src["type"] == "bridge":
            feed = fetch_rssbridge(src["url"])
            parsed = parse_atom(feed, src["name"])
            filtered = filter_last_hours(parsed, hours=48)
            all_items.extend(filtered)

    for item in all_items[:5]:
        print(item)

if __name__ == "__main__":
    main()
