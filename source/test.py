import requests
from pydantic import BaseModel
import sys

class QueryRequest(BaseModel):
    query: str

# Replace with the public IP address and port of your manager instance
MANAGER_PUBLIC_IP = ""  # e.g., "54.123.45.67"
MANAGER_PORT = 8000  # The port where the manager's FastAPI app is running
MANAGER_URL = f"http://{MANAGER_PUBLIC_IP}:{MANAGER_PORT}/execute"

def send_read_query():
    query = "SELECT * FROM actor LIMIT 5;"
    request_payload = QueryRequest(query=query)
    try:
        response = requests.post(MANAGER_URL, json=request_payload.dict())
        response.raise_for_status()
        print("Read Query Response:")
        print(response.json())
    except requests.RequestException as exc:
        print(f"An error occurred: {exc}")

def send_write_query():
    query = "INSERT INTO actor (first_name, last_name, last_update) VALUES ('John', 'Doe', NOW());"
    request_payload = QueryRequest(query=query)
    try:
        response = requests.post(MANAGER_URL, json=request_payload.dict())
        response.raise_for_status()
        print("Write Query Response:")
        print(response.json())
    except requests.RequestException as exc:
        print(f"An error occurred: {exc}")

def main():
    if len(sys.argv) > 1 and sys.argv[1] == 'write':
        send_write_query()
    else:
        send_read_query()

if __name__ == "__main__":
    main()
