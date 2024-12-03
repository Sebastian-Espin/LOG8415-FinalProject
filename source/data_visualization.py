import re
import glob
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict


def visualize_data():
    #manager graph
    file_path = "source/data/manager_output.txt"  
    with open(file_path, 'r') as file:
        logs = file.readlines()

    # Extract write and read requests
    write_pattern = re.compile(r"Executing query: INSERT")
    read_pattern = re.compile(r"Executing query: SELECT")

    read_count = len([line for line in logs if read_pattern.search(line)])
    write_count = len([line for line in logs if write_pattern.search(line)])

    # Plot the data
    plt.bar(["Read Requests", "Write Requests"], [read_count, write_count])
    plt.title("Read and Write Request Executed on the Manager")
    plt.ylabel("Count")
    plt.show()

    #proxy graph
    file_path = "source/data/proxy_output.txt" 
    with open(file_path, 'r') as file:
        logs = file.readlines()

    # Capture request type and worker IP
    ping_pattern = re.compile(r"Ping-Based request: Forwarding to Worker at (\d+\.\d+\.\d+\.\d+)")
    random_pattern = re.compile(r"Random request: Forwarding to Worker at (\d+\.\d+\.\d+\.\d+)")

    # Count occurrences of each type of request per worker IP
    request_counts = defaultdict(lambda: {"Ping-Based": 0, "Random": 0})

    for line in logs:
        ping_match = ping_pattern.search(line)
        random_match = random_pattern.search(line)
        if ping_match:
            ip = ping_match.group(1)
            request_counts[ip]["Ping-Based"] += 1
        elif random_match:
            ip = random_match.group(1)
            request_counts[ip]["Random"] += 1

    worker_ips = list(request_counts.keys())
    ping_counts = [request_counts[ip]["Ping-Based"] for ip in worker_ips]
    random_counts = [request_counts[ip]["Random"] for ip in worker_ips]

    x = range(len(worker_ips))  # Worker IP indices
    plt.bar(x, ping_counts, width=0.4, label="Ping-Based", align="center")
    plt.bar(x, random_counts, width=0.4, label="Random", align="edge")
    plt.xticks(x, worker_ips, rotation=45, ha="right")
    plt.title("Read Requests Sent to Workers by the Proxy")
    plt.xlabel("Worker IPs")
    plt.ylabel("Number of Requests")
    plt.legend()
    plt.tight_layout()
    plt.show()

    #worker graphs
    log_files = glob.glob("source/data/worker_output_*.txt")  

    if not log_files:
        print("No worker log files found.")
    else:
        worker_data = {}

        for file_name in log_files:
            worker_name = file_name.split("worker_output_")[-1].replace(".txt", "").replace("_", ".")
            with open(file_name, 'r') as file:
                logs = file.readlines()

            write_pattern = re.compile(r"Executing query: INSERT")
            read_pattern = re.compile(r"Executing query: SELECT")

            read_count = len([line for line in logs if read_pattern.search(line)])
            write_count = len([line for line in logs if write_pattern.search(line)])
            worker_data[worker_name] = {"Read": read_count, "Write": write_count}

    workers = list(worker_data.keys())
    read_counts = [worker_data[worker]["Read"] for worker in workers]
    write_counts = [worker_data[worker]["Write"] for worker in workers]

    x = np.arange(len(workers)) 
    width = 0.35 

    fig, ax = plt.subplots()
    ax.bar(x - width/2, read_counts, width, label="Read Requests")
    ax.bar(x + width/2, write_counts, width, label="Write Requests")

    ax.set_xlabel("Workers")
    ax.set_ylabel("Request Count")
    ax.set_title("Read and Write Requests Executed on Workers")
    ax.set_xticks(x)
    ax.set_xticklabels(workers, rotation=45, ha="right")
    ax.legend()

    fig.tight_layout()
    plt.show()
