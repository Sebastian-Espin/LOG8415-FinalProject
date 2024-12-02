import asyncio
import httpx
import time
import argparse
from pydantic import BaseModel

class QueryRequest(BaseModel):
    query: str
    strategy: str = None  # Strategy is optional for write requests

GATEKEEPER_PUBLIC_IP = "54.90.172.208"  
GATEKEEPER_PORT = 8000  
GATEKEEPER_BASE_URL = f"http://{GATEKEEPER_PUBLIC_IP}:{GATEKEEPER_PORT}"

async def send_request(client, endpoint, query, strategy=None):
    url = f"{GATEKEEPER_BASE_URL}{endpoint}"
    request_payload = {"query": query}
    if strategy:
        request_payload["strategy"] = strategy
    try:
        response = await client.post(url, json=request_payload, timeout=1000.0)
        response.raise_for_status()
        return True, response.elapsed.total_seconds()
    except httpx.RequestError as exc:
        return False, None
    except httpx.HTTPStatusError as exc:
        return False, None

async def benchmark(endpoint, request_type, num_requests, strategy=None):
    async with httpx.AsyncClient() as client:
        tasks = []
        for i in range(num_requests):
            if request_type == 'read':
                query = f"SELECT * FROM actor LIMIT 1 OFFSET {i % 100};"
                tasks.append(send_request(client, endpoint, query, strategy))
            else:  # write
                query = f"INSERT INTO actor (first_name, last_name, last_update) VALUES ('Test{i}', 'User{i}', NOW());"
                tasks.append(send_request(client, endpoint, query, strategy))
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

    print(f"Benchmark Results ({request_type} requests):")
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
    parser = argparse.ArgumentParser(description='Benchmark script for Gatekeeper.')
    parser.add_argument('--endpoint', type=str, default='/request')
    parser.add_argument('--request_type', type=str, choices=['read', 'write'], default='read')
    parser.add_argument('--num_requests', type=int, default=100)
    parser.add_argument('--strategy', type=str, choices=['direct-hit', 'random', 'ping-based'])
    args = parser.parse_args()

    if args.request_type == 'read' and not args.strategy:
        parser.error("--strategy is required for read requests.")

    endpoint = args.endpoint
    asyncio.run(benchmark(endpoint, args.request_type, args.num_requests, args.strategy))

if __name__ == '__main__':
    main()
