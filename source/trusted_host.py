from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx

app = FastAPI()

class QueryRequest(BaseModel):
    query: str
    strategy: str

PROXY_IP = "{proxy_ip}"
PROXY_BASE_URL = f"http://{{PROXY_IP}}:8000"

@app.post("/process")
async def process_request(request: QueryRequest):
    # Extract the strategy
    strategy = request.strategy
    if strategy not in ["direct-hit", "random", "ping-based"]:
        raise HTTPException(status_code=400, detail="Invalid strategy. Must be 'direct-hit', 'random', or 'ping-based'.")

    # Determine the Proxy endpoint based on the strategy
    proxy_url = f"{{PROXY_BASE_URL}}/{{strategy}}"

    # Prepare the request data (exclude 'strategy' as it's no longer needed)
    request_data = {{"query": request.query}}

    # Forward the request to the Proxy
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(proxy_url, json=request_data, timeout=1000.0)
            response.raise_for_status()
            return response.json()
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Error forwarding request to Proxy: {{exc}}") from exc
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"Error from Proxy: {{exc.response.text}}") from exc
