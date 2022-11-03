# example client for talking to linkage module

import socket
import time

HOST = "127.0.0.1"
PORT = 6666

time.sleep(10)
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    s.sendall(b"hello server\n")
    data = s.recv(1024)

print(f"received {data!r}")

while(True):  # for keeping container alive
    time.sleep(10)