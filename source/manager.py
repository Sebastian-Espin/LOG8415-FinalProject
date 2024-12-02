from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import mysql.connector
import logging
import httpx
import asyncio

app = FastAPI()

class QueryRequest(BaseModel):
    query: str

DB_USER = "root"
DB_PASSWORD = "SomePassword123"
DB_NAME = "sakila"
DB_HOST = "localhost"

# List of worker instance IPs
WORKER_IPS = [{workers_str}]  # Replace with actual IPs
WORKER_URLS = [f"http://{{ip}}:8000/execute" for ip in WORKER_IPS]

def connect_to_mysql():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return connection
    except mysql.connector.Error as e:
        logging.error(f"Database connection failed: {{e}}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {{e}}")

# Set up basic logging
logging.basicConfig(
    filename='/home/ubuntu/request_log.log',  # Path to log file
    level=logging.INFO,  # Log level
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    # Log request details
    client_host = request.client.host
    request_body = await request.body()
    logging.info(f"Incoming request from {{client_host}}: {{request.method}} {{request.url}} Body: {{request_body.decode('utf-8')}}")
    
    # Process the request
    response = await call_next(request)
    
    # Log response details
    logging.info(f"Response status: {{response.status_code}}")
    return response

@app.post("/execute")
async def execute_query(request: QueryRequest):
    query = request.query
    logging.info(f"Executing query: {{query}}")
    connection = connect_to_mysql()
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        if query.strip().lower().startswith("select"):
            result = cursor.fetchall()
            connection.close()
            logging.info(f"Query result: {{result}}")
            return {{"status": "success", "data": result}}
        else:
            connection.commit()
            connection.close()
            logging.info("Query executed successfully locally.")

            # Forward the write query to all worker instances
            await forward_write_query_to_workers(query)

            return {{"status": "success", "message": "Query executed successfully on manager and workers."}}
    except mysql.connector.Error as e:
        connection.close()
        logging.error(f"Query failed: {{e}}")
        raise HTTPException(status_code=400, detail=f"Query failed: {{e}}")

async def forward_write_query_to_workers(query):
    async with httpx.AsyncClient() as client:
        tasks = []
        for url in WORKER_URLS:
            tasks.append(send_write_query_to_worker(client, url, query))
        results = await asyncio.gather(*tasks, return_exceptions=True)

    # Log the results of forwarding
    for worker_url, result in zip(WORKER_URLS, results):
        if isinstance(result, Exception):
            logging.error(f"Failed to forward query to {{worker_url}}: {{result}}")
        else:
            logging.info(f"Successfully forwarded query to {{worker_url}}")

async def send_write_query_to_worker(client, url, query):
    request_payload = {{"query": query}}
    try:
        response = await client.post(url, json=request_payload, timeout=10.0)
        response.raise_for_status()
        return response.json()
    except httpx.RequestError as exc:
        raise Exception(f"An error occurred while requesting {{exc.request.url!r}}: {{exc}}") from exc
    except httpx.HTTPStatusError as exc:
        raise Exception(f"HTTP error {{exc.response.status_code}} while requesting {{exc.request.url!r}}: {{exc.response.text}}") from exc