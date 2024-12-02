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

