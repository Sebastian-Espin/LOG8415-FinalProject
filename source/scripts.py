# Setup scripts for ec2 instances
SQL_script = '''#!/bin/bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y python3 python3-pip python3-venv mysql-server wget unzip
sudo apt-get install -y sysbench 

cd /home/ubuntu
wget https://downloads.mysql.com/docs/sakila-db.zip
unzip sakila-db.zip

# Create the MySQL general log file
sudo touch /var/log/mysql/mysql.log
sudo chown mysql:mysql /var/log/mysql/mysql.log
sudo chmod 640 /var/log/mysql/mysql.log

# Configure MySQL general log
sudo sed -i '/\[mysqld\]/a general_log = 1\ngeneral_log_file = /var/log/mysql/mysql.log' /etc/mysql/mysql.conf.d/mysqld.cnf

# Allow MySQL to listen on all network interfaces
sudo sed -i '/bind-address/s/^#//g' /etc/mysql/mysql.conf.d/mysqld.cnf
sudo sed -i '/bind-address/s/127.0.0.1/0.0.0.0/' /etc/mysql/mysql.conf.d/mysqld.cnf

# Start MySQL service
sudo systemctl start mysql
sudo systemctl enable mysql

ROOT_PASSWORD="SomePassword123"  

# Automate the mysql_secure_installation steps
sudo mysql -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH 'mysql_native_password' BY '$ROOT_PASSWORD';"
sudo mysql -e "CREATE USER 'root'@'%' IDENTIFIED BY '$ROOT_PASSWORD';" # Create a remote root user
sudo mysql -e "GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;" # Grant privileges for remote access
sudo mysql -e "FLUSH PRIVILEGES;"                     # Reload privilege tables

# Load the Sakila database into MySQL
mysql -u root -p"$ROOT_PASSWORD" -e "CREATE DATABASE sakila;"
mysql -u root -p"$ROOT_PASSWORD" sakila < sakila-db/sakila-schema.sql
mysql -u root -p"$ROOT_PASSWORD" sakila < sakila-db/sakila-data.sql

# Run sysbench
sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user="root" --mysql-password="$ROOT_PASSWORD" prepare
sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user="root" --mysql-password="$ROOT_PASSWORD" run > sysbench_output.txt

python3 -m venv /home/ubuntu/venv
source /home/ubuntu/venv/bin/activate
pip install fastapi uvicorn mysql-connector-python

# Create the FastAPI application
cat <<EOF > /home/ubuntu/manager_app.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mysql.connector

app = FastAPI()

class QueryRequest(BaseModel):
    query: str

DB_USER = "root"
DB_PASSWORD = "SomePassword123"
DB_NAME = "sakila"
DB_HOST = "localhost"

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
        raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")

@app.post("/execute")
def execute_query(request: QueryRequest):
    query = request.query
    connection = connect_to_mysql()
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        if query.strip().lower().startswith("select"):
            result = cursor.fetchall()
            connection.close()
            return {"status": "success", "data": result}
        else:
            connection.commit()
            connection.close()
            return {"status": "success", "message": "Query executed successfully."}
    except mysql.connector.Error as e:
        connection.close()
        raise HTTPException(status_code=400, detail=f"Query failed: {e}")
EOF

# Start the FastAPI server
nohup /home/ubuntu/venv/bin/uvicorn manager_app:app --host 0.0.0.0 --port 8000 > /home/ubuntu/fastapi.log 2>&1 &
'''



def update_proxy_script(manager_ip, worker_ips):
    workers_str = ', '.join([f'"{ip}"' for ip in worker_ips])
    proxy_script = f'''#!/bin/bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y python3 python3-pip python3-venv
python3 -m venv /home/ubuntu/venv
source /home/ubuntu/venv/bin/activate
pip install fastapi uvicorn httpx

# Create the FastAPI application
cat <<EOF > /home/ubuntu/proxy_app.py
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
EOF

# Start the FastAPI server
cd /home/ubuntu
nohup /home/ubuntu/venv/bin/uvicorn proxy_app:app --host 0.0.0.0 --port 8000 > /home/ubuntu/fastapi.log 2>&1 &
'''
    return proxy_script

