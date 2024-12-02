# Setup scripts for ec2 instances
worker_script = '''#!/bin/bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y python3 python3-pip python3-venv mysql-server wget unzip
sudo apt-get install -y sysbench 

cd /home/ubuntu
wget https://downloads.mysql.com/docs/sakila-db.zip
unzip sakila-db.zip

# Create the MySQL general log file
sudo touch /home/ubuntu/request_log.log
sudo chmod 666 /home/ubuntu/request_log.log

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
cat <<EOF > /home/ubuntu/worker_app.py
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import mysql.connector
import logging

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
        logging.error(f"Database connection failed: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {e}")

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
    logging.info(f"Incoming request from {client_host}: {request.method} {request.url} Body: {request_body.decode('utf-8')}")
    
    # Process the request
    response = await call_next(request)
    
    # Log response details
    logging.info(f"Response status: {response.status_code}")
    return response

@app.post("/execute")
def execute_query(request: QueryRequest):
    query = request.query
    logging.info(f"Executing query: {query}")
    connection = connect_to_mysql()
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        if query.strip().lower().startswith("select"):
            result = cursor.fetchall()
            connection.close()
            logging.info(f"Query result: {result}")
            return {"status": "success", "data": result}
        else:
            connection.commit()
            connection.close()
            logging.info("Query executed successfully.")
            return {"status": "success", "message": "Query executed successfully."}
    except mysql.connector.Error as e:
        connection.close()
        logging.error(f"Query failed: {e}")
        raise HTTPException(status_code=400, detail=f"Query failed: {e}")
EOF

# Start the FastAPI server
nohup /home/ubuntu/venv/bin/uvicorn worker_app:app --host 0.0.0.0 --port 8000 > /home/ubuntu/fastapi.log 2>&1 &
'''



def update_manager_script(worker_ips):
    workers_str = ', '.join([f'"{ip}"' for ip in worker_ips])
    manager_script = f'''#!/bin/bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y python3 python3-pip python3-venv mysql-server wget unzip
sudo apt-get install -y sysbench 

cd /home/ubuntu
wget https://downloads.mysql.com/docs/sakila-db.zip
unzip sakila-db.zip

# Create the MySQL general log file
sudo touch /home/ubuntu/request_log.log
sudo chmod 666 /home/ubuntu/request_log.log

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
pip install fastapi uvicorn mysql-connector-python httpx

# Create the FastAPI application
cat <<EOF > /home/ubuntu/manager_app.py
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
        response = await client.post(url, json=request_payload, timeout=1000.0)
        response.raise_for_status()
        return response.json()
    except httpx.RequestError as exc:
        raise Exception(f"An error occurred while requesting {{exc.request.url!r}}: {{exc}}") from exc
    except httpx.HTTPStatusError as exc:
        raise Exception(f"HTTP error {{exc.response.status_code}} while requesting {{exc.request.url!r}}: {{exc.response.text}}") from exc
EOF

nohup /home/ubuntu/venv/bin/uvicorn manager_app:app --host 0.0.0.0 --port 8000 > /home/ubuntu/fastapi.log 2>&1 &
'''
    return manager_script



def update_proxy_script(manager_ip, worker_ips):
    workers_str = ', '.join([f'"{ip}"' for ip in worker_ips])
    proxy_script = f'''#!/bin/bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y python3 python3-pip python3-venv

sudo touch /home/ubuntu/request_log.log
sudo chmod 666 /home/ubuntu/request_log.log

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
EOF

# Start the FastAPI server
cd /home/ubuntu
nohup /home/ubuntu/venv/bin/uvicorn proxy_app:app --host 0.0.0.0 --port 8000 > /home/ubuntu/fastapi.log 2>&1 &
'''
    return proxy_script



def update_trusted_host_script(proxy_ip):
    trusted_host_script = f'''#!/bin/bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y python3 python3-pip python3-venv
python3 -m venv /home/ubuntu/venv
source /home/ubuntu/venv/bin/activate
pip install fastapi uvicorn httpx

# Create the FastAPI application
cat <<EOF > /home/ubuntu/trusted_host_app.py
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
EOF

# Start the FastAPI server
cd /home/ubuntu
nohup /home/ubuntu/venv/bin/uvicorn trusted_host_app:app --host 0.0.0.0 --port 8000 > /home/ubuntu/trusted_host_app.log 2>&1 &
'''
    return trusted_host_script


def update_gatekeeper_script(trusted_host_ip):
    gatekeeper_script = f'''#!/bin/bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y python3 python3-pip python3-venv
python3 -m venv /home/ubuntu/venv
source /home/ubuntu/venv/bin/activate
pip install fastapi uvicorn httpx

# Create the FastAPI application
cat <<EOF > /home/ubuntu/gatekeeper_app.py
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
            response = await client.post(TRUSTED_HOST_URL, json=request_data, timeout=1000.0)
            response.raise_for_status()
            return response.json()
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"Error forwarding request to Trusted Host: {{exc}}") from exc
    except httpx.HTTPStatusError as exc:
        raise HTTPException(status_code=exc.response.status_code, detail=f"Error from Trusted Host: {{exc.response.text}}") from exc
EOF

# Start the FastAPI server
cd /home/ubuntu
nohup /home/ubuntu/venv/bin/uvicorn gatekeeper_app:app --host 0.0.0.0 --port 8000 > /home/ubuntu/gatekeeper_app.log 2>&1 &
'''

    return gatekeeper_script