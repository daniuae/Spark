# Use this code instead of the netcat
#Use Python as a Socket Server
#You can replace nc -lk 9999 with a simple Python socket server.
#Create a file socket_server.py:
#python socket_server.py

import socket
import time

HOST = "localhost"  
PORT = 9999  

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(1)
print(f"Listening on {PORT}...")

conn, addr = s.accept()
print("Connection from", addr)

with conn:
    while True:
        # send messages continuously
        msg = f"Hello Spark {time.time()}\n"
        conn.sendall(msg.encode("utf-8"))
        time.sleep(2)

https://spark.apache.org/docs/4.0.0/streaming-programming-guide.html
