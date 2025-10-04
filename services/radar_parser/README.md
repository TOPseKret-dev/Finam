# **Чтобы вызвать парсер извне:**
```Python
import asyncio, json
from services.radar_parser.llm_async_adapter import build_llm_payload

payload = asyncio.run(build_llm_payload(hours=48))
print(json.dumps(payload, ensure_ascii=False, indent=2))
```
