from socket import *  # 导入 socket 模块
from threading import Thread
import os

NSADDRESS = ('192.168.1.161', 8765) # address of the nameserver
def recvAll(sock):
    size = 0
    BUFFER_SIZE = 4096 # 4KB
    data = b''
    while True:
        part = sock.recv(BUFFER_SIZE)
        data += part
        size+=len(part)
        if len(part) < BUFFER_SIZE:
            break
    return data, size

if __name__ == "__main__":
    nameserver = socket()
    nameserver.connect(NSADDRESS)
    print(nameserver.recv(1024).decode(encoding="utf-8"))

    while True:
        print("- Usage:")
        print("-    0. list existing files: ls")
        print("-    1. put file: put <pathToFile>")
        print("-    2. get file: get <fileName>")
        print("-    3. exit")
        command = str(input()).split(maxsplit=1)
        if command[0] == "ls":
            nameserver.send(b'0')
            msg, _ = recvAll(nameserver)
            print(msg.decode())

        elif command[0] == "put":
            if len(command) != 2:
                print("put file command: put <pathToFile>")
                continue
            path = command[1]
            _, fileName = os.path.split(path)
            if not os.path.isfile(fileName):
                print("No such file!")
                continue
            nameserver.send(("1"+fileName).encode())
            state = nameserver.recv(128).decode(encoding="utf-8")
            if state != "ok":
                print("Upload error:", state)
                continue
            
            with open(path, 'rb') as f:
                msg = f.read()
            nameserver.sendall(msg)
            state = nameserver.recv(128).decode(encoding="utf-8")
            if state== "ok":
                print("Upload successfully!")
            else:
                print("Upload error:", state)

        elif command[0] == "get":
            if len(command) != 2:
                print("get file command: get <fileName>")
                continue
            fileName = command[1]
            if os.path.exists("./"+fileName):
                print("File:", fileName, "already exists!")
                continue
            nameserver.send(("2"+fileName).encode())

            # receive the file
            state = nameserver.recv(128).decode(encoding="utf-8")
            if state != "ok":
                print(state) # fetch error
                continue
            content, _ = recvAll(nameserver)
            with open(fileName, 'wb') as f:
                f.write(content)
            print("Get file successfully!")

        elif command[0] == "exit":
            nameserver.send("3".encode())
            nameserver.close()
            break