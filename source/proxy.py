from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import random
import httpx
import asyncio
import time
import logging

app = FastAPI()

logging.basicConfig(
    filename='/home/ubuntu/request_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class QueryRequest(BaseModel):
    query: str

MANAGER_IP = "{manager_ip}"
WORKER_NODES = [{workers_str}]
MANAGER_URL = f"http://{{MANAGER_IP}}:8000/execute"
WORKER_URLS = [f"http://{{ip}}:8000/execute" for ip in WORKER_NODES]

async def forward_request(url, request_data):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=request_data, timeout=1000.0)
            response.raise_for_status()
            return response.json()
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Error forwarding request: {{exc}}") from exc
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"Error from backend: {{exc.response.text}}") from exc

@app.post("/direct-hit")
async def direct_hit(request: QueryRequest):
    # Forward the request to the manager
    request_data = request.dict()
    logging.info(f"Direct-Hit request: Forwarding to Manager at {{MANAGER_IP}}")
    return await forward_request(MANAGER_URL, request_data)

@app.post("/random")
async def random_query(request: QueryRequest):
    # Select a random worker
    worker_url = random.choice(WORKER_URLS)
    worker_ip = worker_url.split('//')[1].split(':')[0]
    request_data = request.dict()
    logging.info(f"Random request: Forwarding to Worker at {{worker_ip}}")
    return await forward_request(worker_url, request_data)

@app.post("/ping-based")
async def ping_based_endpoint(request: QueryRequest):
    try:
        # Measure ping times
        ping_tasks = {{ip: tcp_ping(ip) for ip in WORKER_NODES}}
        ping_results = await asyncio.gather(*ping_tasks.values())
        
        ping_times = {{}}
        for ip, result in zip(ping_tasks.keys(), ping_results):
            if result is not None:
                ping_times[ip] = result
            else:
                logging.warning(f"Worker {{ip}} is unreachable during ping")
        
        if not ping_times:
            raise HTTPException(status_code=503, detail="No reachable workers.")
        
        # Select the worker with the lowest ping time
        best_worker_ip = min(ping_times, key=ping_times.get)
        best_worker_url = f"http://{{best_worker_ip}}:8000/execute"
        
        # Prepare the request data
        request_data = request.dict()
        logging.info(f"Ping-Based request: Forwarding to Worker at {{best_worker_ip}}")

        # Forward the request to the selected worker
        result = await forward_request(best_worker_url, request_data)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in ping-based endpoint: {{e}}")

async def tcp_ping(ip, port=8000, timeout=1):
    start_time = time.time()
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(ip, port), timeout)
        elapsed_time = time.time() - start_time
        writer.close()
        await writer.wait_closed()
        return elapsed_time
    except Exception:
        return None