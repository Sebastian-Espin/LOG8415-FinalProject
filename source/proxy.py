from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import random
import mysql.connector
import subprocess

app = FastAPI()

class QueryRequest(BaseModel):
    query: str

DB_USER = "root"
DB_PASSWORD = "SomePassword123"
DB_NAME = "sakila"

MANAGER_IP = "{manager_ip}"
WORKER_NODES = [{workers_str}]

def connect_to_mysql(host):
    try:
        connection = mysql.connector.connect(
            host=host,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        return connection
    except mysql.connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Database connection failed: {{e}}")

@app.post("/direct-hit")
def direct_hit_query(request: QueryRequest):
    query = request.query
    connection = connect_to_mysql(MANAGER_IP)
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    connection.close()
    return {{"status": "success", "data": result}}

@app.post("/random")
def random_query(query: str):
    worker_ip = random.choice(WORKER_NODES)
    connection = connect_to_mysql(worker_ip)
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    connection.close()
    return {{"status": "success", "worker": worker_ip, "data": result}}

@app.post("/ping-based")
def ping_based_query(query: str):
    best_worker = None
    lowest_ping = float("inf")

    for worker_ip in WORKER_NODES:
        try:
            result = subprocess.run(
                ["ping", "-c", "1", worker_ip],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            if result.returncode == 0:
                ping_time = float(result.stdout.split("time=")[-1].split(" ")[0])
                if ping_time < lowest_ping:
                    lowest_ping = ping_time
                    best_worker = worker_ip
        except Exception as e:
            print(f"Failed to ping {{worker_ip}}: {{e}}")

    if not best_worker:
        raise HTTPException(status_code=500, detail="No available workers based on ping time.")

    connection = connect_to_mysql(best_worker)
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    connection.close()
    return {{"status": "success", "worker": best_worker, "data": result}}