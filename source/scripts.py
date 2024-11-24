# Setup scripts for ec2 instances
SQL_script = '''#!/bin/bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y python3 python3-pip mysql-server wget unzip
sudo apt-get install -y sysbench 

cd /home/ubuntu
wget https://downloads.mysql.com/docs/sakila-db.zip
unzip sakila-db.zip

# Start MySQL service
sudo systemctl start mysql
sudo systemctl enable mysql

ROOT_PASSWORD="SomePassword123"  

# Automate the mysql_secure_installation steps
sudo mysql -e "ALTER USER 'root'@'localhost' IDENTIFIED WITH 'mysql_native_password' BY '$ROOT_PASSWORD';"
sudo mysql -e "DELETE FROM mysql.user WHERE User='';"  # Remove anonymous users
sudo mysql -e "DROP DATABASE IF EXISTS test;"          # Remove the test database
sudo mysql -e "DELETE FROM mysql.db WHERE Db='test' OR Db='test\\_%';"
sudo mysql -e "UPDATE mysql.user SET Host='localhost' WHERE User='root';"  # Disable remote root login
sudo mysql -e "FLUSH PRIVILEGES;"                     # Reload privilege tables


# Load the Sakila database into MySQL
mysql -u root -p"$ROOT_PASSWORD" -e "CREATE DATABASE sakila;"
mysql -u root -p"$ROOT_PASSWORD" sakila < sakila-db/sakila-schema.sql
mysql -u root -p"$ROOT_PASSWORD" sakila < sakila-db/sakila-data.sql

# Run sysbench
sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user="root" --mysql-password="$ROOT_PASSWORD" prepare
sudo sysbench /usr/share/sysbench/oltp_read_only.lua --mysql-db=sakila --mysql-user="root" --mysql-password="$ROOT_PASSWORD" run > sysbench_output.txt
'''



def update_proxy_script(manager_ip, worker_ips):
    workers_str = ', '.join([f'"{ip}"' for ip in worker_ips])
    proxy_script = f'''#!/bin/bash
sudo apt-get update && sudo apt-get upgrade -y
sudo apt-get install -y python3 python3-pip python3-venv
python3 -m venv /home/ubuntu/venv
source /home/ubuntu/venv/bin/activate
pip install fastapi uvicorn mysql-connector-python

# Create the FastAPI application
cat <<EOF > /home/ubuntu/proxy_app.py
from fastapi import FastAPI, HTTPException
import random
import mysql.connector
import subprocess

app = FastAPI()

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
def direct_hit_query(query: str):
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
EOF

# Start the FastAPI server
cd /home/ubuntu
nohup /home/ubuntu/venv/bin/uvicorn proxy_app:app --host 0.0.0.0 --port 8000 > /home/ubuntu/fastapi.log 2>&1 &
'''
    return proxy_script

