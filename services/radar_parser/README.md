# **Чтобы вызвать парсер извне:**
```Python
from pipeline import run_pipeline

res = run_pipeline(hours=48)  # по умолчанию возьмёт <project_root>/config/sources.csv
print(res)
```
# **Проверка источников**
```python
from pipeline import run_pipeline

res = run_pipeline(hours=48)
from collections import Counter, defaultdict
import pprint as pp

counts = Counter(it["source"] for it in res["items"])
print("\n=== by_source (после фильтров) ===")
for src, n in counts.most_common():
    print(f"{src:25} {n:3}")

print("\n=== errors (сырые) ===")
for e in res.get("errors", []):
    print("•", e.get("error"))


zero_like = defaultdict(int)
for e in res.get("errors", []):
    if " for url: " in (msg := e.get("error","")):
        src_url = msg.split(" for url: ", 1)[-1]
        zero_like[src_url] += 1
if zero_like:
    print("\n=== подозрение на «0 результатов»/ошибки по URL ===")
    for u, k in zero_like.items():
        print(f"{u}  (errors: {k})")

```
