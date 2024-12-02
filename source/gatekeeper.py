from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import re

app = FastAPI()

class QueryRequest(BaseModel):
    query: str
    strategy: str  

TRUSTED_HOST_IP = "{trusted_host_ip}" 
TRUSTED_HOST_URL = f"http://{{TRUSTED_HOST_IP}}:8000/process"

def validate_query(query: str) -> bool:
    # only certain SQL statements
    pattern = re.compile(r"^\s*(SELECT|INSERT|UPDATE|DELETE|REPLACE|ALTER|CREATE|DROP|TRUNCATE)\s", re.IGNORECASE)
    return bool(pattern.match(query))

@app.post("/request")
async def handle_request(request: QueryRequest):
    # Validate the query
    if not validate_query(request.query):
        raise HTTPException(status_code=400, detail="Invalid query.")

    # Prepare the request data
    request_data = request.dict()

    # Forward the request to the Trusted Host
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(TRUSTED_HOST_URL, json=request_data, timeout=10.0)
            response.raise_for_status()
            return response.json()
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Error forwarding request to Trusted Host: {{exc}}") from exc
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"Error from Trusted Host: {{exc.response.text}}") from exc
