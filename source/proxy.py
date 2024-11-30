from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import random
import httpx
import asyncio

app = FastAPI()

class QueryRequest(BaseModel):
    query: str

MANAGER_IP = "{manager_ip}"
WORKER_NODES = [{workers_str}]
MANAGER_URL = f"http://{{MANAGER_IP}}:8000/execute"
WORKER_URLS = [f"http://{{ip}}:8000/execute" for ip in WORKER_NODES]

async def forward_request(url, request: Request):
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=await request.json())
            response.raise_for_status()
            return response.json()
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Error forwarding request: {{exc}}") from exc
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"Error from backend: {{exc.response.text}}") from exc

@app.post("/direct-hit")
async def direct_hit(request: Request):
    # Forward the request to the manager
    return await forward_request(MANAGER_URL, request)

@app.post("/random")
async def random_query(request: Request):
    # Select a random worker
    worker_url = random.choice(WORKER_URLS)
    return await forward_request(worker_url, request)

@app.post("/ping-based")
async def ping_based_query(request: Request):
    # Find the worker with the lowest ping
    best_worker = None
    lowest_ping = float("inf")

    ping_tasks = []
    for ip in WORKER_NODES:
        ping_tasks.append(get_ping_time(ip))

    ping_times = await asyncio.gather(*ping_tasks)

    for ip, ping_time in zip(WORKER_NODES, ping_times):
        if ping_time is not None and ping_time < lowest_ping:
            lowest_ping = ping_time
            best_worker = ip

    if not best_worker:
        raise HTTPException(status_code=500, detail="No available workers based on ping time.")

    worker_url = f"http://{{best_worker}}:8000/execute"
    return await forward_request(worker_url, request)

async def get_ping_time(ip):
    proc = await asyncio.create_subprocess_exec(
        'ping', '-c', '1', ip,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode == 0:
        output = stdout.decode()
        try:
            ping_time = float(output.split('time=')[-1].split(' ')[0])
            return ping_time
        except (IndexError, ValueError):
            return None
    else:
        return None