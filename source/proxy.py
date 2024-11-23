from fastapi import FastAPI, HTTPException
import random
import mysql.connector
import subprocess

app = FastAPI()

# Database credentials and worker nodes
DB_USER = "root"
DB_PASSWORD = "YourRootPassword"
DB_NAME = "sakila"

MANAGER_IP = "MANAGER_IP_PLACEHOLDER"
WORKER_NODES = ["WORKER1_IP_PLACEHOLDER", "WORKER2_IP_PLACEHOLDER"]

# Function to connect to a MySQL server
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
        raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")

@app.post("/direct-hit")
def direct_hit_query(query: str):
    """
    Directly send all requests to the Manager database.
    """
    connection = connect_to_mysql(MANAGER_IP)
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    connection.close()
    return {"status": "success", "data": result}

@app.post("/random")
def random_query(query: str):
    """
    Send requests to a randomly selected worker node for reads.
    """
    worker_ip = random.choice(WORKER_NODES)
    connection = connect_to_mysql(worker_ip)
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    connection.close()
    return {"status": "success", "worker": worker_ip, "data": result}

@app.post("/ping-based")
def ping_based_query(query: str):
    """
    Send requests to the worker node with the lowest ping time.
    """
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
            print(f"Failed to ping {worker_ip}: {e}")

    if not best_worker:
        raise HTTPException(status_code=500, detail="No available workers based on ping time.")

    # Send query to the best worker
    connection = connect_to_mysql(best_worker)
    cursor = connection.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    connection.close()
    return {"status": "success", "worker": best_worker, "data": result}