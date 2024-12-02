import asyncio
import httpx
import time

WORKER_IPS = ["34.224.21.198", "54.91.194.139"] 
WORKER_PORT = 8000  
WORKER_ENDPOINT = "/execute" 

# The SQL query to execute
QUERY = "SELECT * FROM actor LIMIT 1;"  # Modify the query as needed

async def tcp_ping(ip, port=8000, timeout=1):
    """Asynchronously measure the latency to a given IP and port."""
    start_time = time.time()
    try:
        reader, writer = await asyncio.open_connection(ip, port)
        elapsed_time = time.time() - start_time
        writer.close()
        await writer.wait_closed()
        return elapsed_time
    except Exception:
        return None

async def get_ping_times(worker_ips):
    """Ping all workers and return a dictionary of IPs and their ping times."""
    ping_tasks = {ip: tcp_ping(ip, port=WORKER_PORT) for ip in worker_ips}
    ping_results = await asyncio.gather(*ping_tasks.values())

    ping_times = {}
    for ip, result in zip(ping_tasks.keys(), ping_results):
        if result is not None:
            ping_times[ip] = result
        else:
            print(f"Worker {ip} is unreachable or an error occurred.")
    return ping_times

async def send_request(worker_ip):
    """Send the SQL query to the selected worker and return the response."""
    url = f"http://{worker_ip}:{WORKER_PORT}{WORKER_ENDPOINT}"
    request_data = {"query": QUERY}
    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(url, json=request_data, timeout=10.0)
            response.raise_for_status()
            return response.json()
        except httpx.RequestError as exc:
            print(f"An error occurred while requesting {exc.request.url!r}: {exc}")
        except httpx.HTTPStatusError as exc:
            print(f"HTTP error {exc.response.status_code} while requesting {exc.request.url!r}: {exc.response.text}")
            return None

async def main():
    # Get ping times to all workers
    ping_times = await get_ping_times(WORKER_IPS)
    if not ping_times:
        print("No reachable workers.")
        return

    # Select the worker with the lowest ping time
    best_worker_ip = min(ping_times, key=ping_times.get)
    print(f"Best worker IP: {best_worker_ip} with ping time {ping_times[best_worker_ip]*1000:.2f} ms")

    # Send the query to the selected worker
    result = await send_request(best_worker_ip)
    if result:
        print("Response from worker:")
        print(result)
    else:
        print("Failed to get a response from the worker.")

if __name__ == "__main__":
    asyncio.run(main())

