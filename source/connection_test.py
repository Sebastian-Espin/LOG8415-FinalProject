import mysql.connector

try:
    connection = mysql.connector.connect(
        host='3.91.67.156', 
        user='root',
        password='SomePassword123',
    )
    if connection.is_connected():
        print("Successfully connected to the remote MySQL database!")
except Exception as e:
    print("Error:", e)