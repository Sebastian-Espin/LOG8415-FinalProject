import requests
import time

# FastAPI server details
BASE_URL = "http://<PROXY_IP>:8000"  
# Benchmark parameters
NUM_REQUESTS = 1000

# Sample SQL queries
WRITE_QUERY = "INSERT INTO sakila.actor (first_name, last_name) VALUES ('John', 'Doe');"
READ_QUERY = "SELECT * FROM sakila.actor;"

def send_request(url, query):
    """
    Send a POST request to the specified URL with the given query.
    """
    try:
        response = requests.post(url, json={"query": query})
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return None

def benchmark(proxy_endpoint, query, num_requests):
    """
    Benchmark the specified proxy endpoint with a given query.
    """
    start_time = time.time()
    for i in range(num_requests):
        response = send_request(f"{BASE_URL}{proxy_endpoint}", query)
        if i % 100 == 0:  # Log every 100 requests
            print(f"Request {i + 1}/{num_requests}: {response}")
    end_time = time.time()
    total_time = end_time - start_time
    print(f"Completed {num_requests} requests in {total_time:.2f} seconds.")
    return total_time

def main():
    print("Benchmarking Direct Hit (Writes)...")
    direct_hit_write_time = benchmark("/direct-hit", WRITE_QUERY, NUM_REQUESTS)

    print("\nBenchmarking Direct Hit (Reads)...")
    direct_hit_read_time = benchmark("/direct-hit", READ_QUERY, NUM_REQUESTS)

    print("\nBenchmarking Random Proxy (Reads)...")
    random_proxy_time = benchmark("/random", READ_QUERY, NUM_REQUESTS)

    print("\nBenchmarking Ping-Based Proxy (Reads)...")
    ping_based_time = benchmark("/ping-based", READ_QUERY, NUM_REQUESTS)

    print("\nBenchmark Results:")
    print(f"Direct Hit (Writes): {direct_hit_write_time:.2f} seconds")
    print(f"Direct Hit (Reads): {direct_hit_read_time:.2f} seconds")
    print(f"Random Proxy (Reads): {random_proxy_time:.2f} seconds")
    print(f"Ping-Based Proxy (Reads): {ping_based_time:.2f} seconds")

if __name__ == "__main__":
    main()
