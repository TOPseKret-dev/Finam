from fastapi import FastAPI, Query, HTTPException
from .service import get_top_k

app = FastAPI(title="RADAR")

@app.get("/radar")
async def radar(window: int = Query(24, gt=0), k: int = Query(5, gt=0)):
    if window <= 0 or k <= 0:
        raise HTTPException(status_code=400, detail="window and k must be positive")
    return await get_top_k(window=window, k=k)

@app.get("/health")
async def health():
    return {"status": "ok"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("services.api.src.api.__main__:app", host="127.0.0.1", port=8000, reload=True)
