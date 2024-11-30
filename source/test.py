import mysql.connector
import logging
import time

# Logging configuration
logging.basicConfig(filename="test_request_logs.log", level=logging.INFO, format='%(asctime)s - %(message)s')

# Database connection details
DB_USER = "root"
DB_PASSWORD = "SomePassword123"
MANAGER_IP = "3.91.67.156"  # Replace with the actual manager instance IP

def connect_to_mysql(host):
    """
    Connect to MySQL database on the specified host.
    """
    try:
        connection = mysql.connector.connect(
            host=host,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        logging.info(f"Connected to MySQL on {host}.")
        return connection
    except mysql.connector.Error as e:
        logging.error(f"Failed to connect to MySQL: {e}")
        raise RuntimeError(f"Database connection failed: {e}")

def execute_query(connection, query):
    """
    Execute a single query on the provided database connection.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        if query.strip().lower().startswith("select"):
            result = cursor.fetchall()
            logging.info(f"Query executed successfully: {query}")
            logging.info(f"Result: {result}")
        else:
            connection.commit()
            logging.info(f"Non-SELECT query executed successfully: {query}")
    except mysql.connector.Error as e:
        logging.error(f"MySQL error while executing query: {e}")
        raise RuntimeError(f"MySQL query failed: {e}")

def run_test_queries():
    """
    Run a series of predefined SQL queries to test the database connection and functionality.
    """
    logging.info("Starting automated SQL test queries...")
    queries = [
        "INSERT INTO sakila.actor (first_name, last_name) VALUES ('John', 'Doe');",
        "SELECT * FROM sakila.actor;"
    ]

    try:
        connection = connect_to_mysql(MANAGER_IP)
        for query in queries:
            logging.info(f"Executing query: {query}")
            execute_query(connection, query)
            time.sleep(1)  # Pause between queries for clarity in logs
        connection.close()
        logging.info("All queries executed successfully.")
    except Exception as e:
        logging.error(f"Error during test queries: {e}")

if __name__ == "__main__":
    run_test_queries()
