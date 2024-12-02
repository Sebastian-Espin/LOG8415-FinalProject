import asyncio
import httpx
import time
import argparse
from pydantic import BaseModel

class QueryRequest(BaseModel):
    query: str

# Replace with the public IP address and port of your proxy instance
PROXY_PUBLIC_IP = "3.89.116.77"  
PROXY_PORT = 8000  # The port where the proxy's FastAPI app is running
PROXY_BASE_URL = f"http://{PROXY_PUBLIC_IP}:{PROXY_PORT}"

ENDPOINTS = ['/direct-hit', '/random', '/ping-based']

async def send_request(client, endpoint, query):
    url = f"{PROXY_BASE_URL}{endpoint}"
    request_payload = {"query": query}
    try:
        response = await client.post(url, json=request_payload, timeout=10.0)
        response.raise_for_status()
        return True, response.elapsed.total_seconds()
    except httpx.RequestError as exc:
        print(f"An error occurred while requesting {exc.request.url!r}: {exc}")
        return False, None
    except httpx.HTTPStatusError as exc:
        print(f"HTTP error {exc.response.status_code} while requesting {exc.request.url!r}: {exc.response.text}")
        return False, None

async def benchmark(endpoint, request_type, num_requests):
    async with httpx.AsyncClient() as client:
        tasks = []
        for i in range(num_requests):
            if request_type == 'read':
                query = f"SELECT * FROM actor LIMIT 1 OFFSET {i % 200};"
            else:  # write
                query = f"INSERT INTO actor (first_name, last_name, last_update) VALUES ('Test{i}', 'User{i}', NOW());"
            tasks.append(send_request(client, endpoint, query))

        start_time = time.perf_counter()
        results = await asyncio.gather(*tasks)
        end_time = time.perf_counter()

    success_count = sum(1 for success, _ in results if success)
    failure_count = num_requests - success_count
    total_time = end_time - start_time
    avg_time_per_request = total_time / num_requests

    # Collect response times for successful requests
    response_times = [elapsed for success, elapsed in results if success and elapsed is not None]

    if response_times:
        min_time = min(response_times)
        max_time = max(response_times)
        avg_response_time = sum(response_times) / len(response_times)
    else:
        min_time = max_time = avg_response_time = None

    print(f"Benchmark Results for Endpoint {endpoint} ({request_type} requests):")
    print(f"Total Requests: {num_requests}")
    print(f"Successful Requests: {success_count}")
    print(f"Failed Requests: {failure_count}")
    print(f"Total Time: {total_time:.2f} seconds")
    print(f"Average Time per Request: {avg_time_per_request:.4f} seconds")
    if avg_response_time is not None:
        print(f"Average Response Time: {avg_response_time:.4f} seconds")
        print(f"Min Response Time: {min_time:.4f} seconds")
        print(f"Max Response Time: {max_time:.4f} seconds")
    else:
        print("No successful responses to calculate response times.")

def main():
    parser = argparse.ArgumentParser(description='Benchmark script for proxy.')
    parser.add_argument('--endpoint', type=str, choices=['direct-hit', 'random', 'ping-based'], default='direct-hit',
                        help='Proxy endpoint to test.')
    parser.add_argument('--request_type', type=str, choices=['read', 'write'], default='read',
                        help='Type of requests to send (read or write).')
    parser.add_argument('--num_requests', type=int, default=1000,
                        help='Number of requests to send.')
    args = parser.parse_args()

    endpoint = f"/{args.endpoint}"
    asyncio.run(benchmark(endpoint, args.request_type, args.num_requests))

if __name__ == '__main__':
    main()

