import numpy as np
import matplotlib.pyplot as plt


def extract_times(file_path):
    real_times = []
    execution_times_float = []

    with open(file_path, 'r') as file:
        for line in file:
            parts = line.strip().split()

            if len(parts) > 1 and parts[0] == 'real':
                real_times.append(parts[1])

    for time_str in real_times:
        minutes, seconds = time_str.split('m')
        seconds = seconds.replace('s', '')
        total_seconds = float(minutes) * 60 + float(seconds)
        execution_times_float.append(total_seconds)

    return execution_times_float

def plot_times_exploration(hadoop_time, linux_time):
    plt.bar(['Hadoop', 'Linux'], [hadoop_time[0], linux_time[0]], color=['blue', 'orange'])
    plt.ylabel('Execution Time (seconds)')
    plt.title('Execution Time for Hadoop and Linux')
    plt.tight_layout()
    plt.show()

def plot_times_hadoop_vs_spark(hadoop_times, spark_times):
    execution_times_hadoop = np.array(hadoop_times).reshape(9, 3)
    execution_times_spark = np.array(spark_times).reshape(9, 3)
    average_time_hadoop = np.mean(execution_times_hadoop, axis=1)
    average_time_spark = np.mean(execution_times_spark, axis=1)
    total_average_hadoop = np.mean(average_time_hadoop)
    total_average_spark = np.mean(average_time_spark)
    
    plt.figure(figsize=(10, 5))
    bar_width = 0.35
    x = ['buchanj', 'carman', 'colby', 'cheyneyp', 'del. bumps', 'charlesworth', 'del. lucy', 'del. myfanwy', 'del. penny']

    plt.bar(x, average_time_spark, width=bar_width, label='Spark', color='orange')
    plt.bar(x, average_time_hadoop, width=bar_width, label='Hadoop', color='blue')
    plt.xlabel('Files')
    plt.ylabel('Average Execution Time (seconds)')
    plt.title('Average Execution Time for Hadoop and Spark')
    plt.legend()
    plt.tight_layout()
    plt.show()

    plt.bar(['Hadoop', 'Spark'], [total_average_hadoop, total_average_spark], color=['blue', 'orange'])
    plt.ylabel('Total Average Execution Time (seconds)')
    plt.title('Total Average Execution Time for Hadoop and Spark')
    plt.tight_layout()
    plt.show()

def run_benchmark():
    hadoop_exploration_times = extract_times("hadoop_exploration.txt")
    linux_exploration_times = extract_times("ubuntu_exploration.txt")
    hadoop_execution_times = extract_times("output_hadoop.txt")
    spark_execution_times = extract_times("output_spark.txt")
    plot_times_exploration(hadoop_exploration_times, linux_exploration_times)
    plot_times_hadoop_vs_spark(hadoop_execution_times, spark_execution_times)


if __name__ == "__main__":
    run_benchmark()