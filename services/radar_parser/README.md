# **Чтобы вызвать парсер извне:**
```Python
from pipeline import run_pipeline

res = run_pipeline(hours=48)  # по умолчанию возьмёт <project_root>/config/sources.csv
print(res)
# или явно:
res = run_pipeline(config_path="/etc/myapp/sources.csv", hours=48)
print(res)
```