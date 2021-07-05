import hashlib
from socket import *
from threading import Thread
import os

ADDRESS = ('192.168.1.161', 8712)
  
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
class DataServer:
    def __init__(self):
        self.dataserver = socket(AF_INET, SOCK_STREAM)
        self.dataserver.bind(ADDRESS)
        self.dataserver.listen(5)
        print("Dataserver construction completed.")
    def keepWorking(self):
        t = Thread(target=self.acceptNameServer, args = ())
        t.setDaemon(True)
        t.start()
        while True:
            s = input("Enter [exit] to shut down the dataserver.\n")
            if s == "exit":
                break
    
    def acceptNameServer(self):
        while True:
            nameserver, _ = self.dataserver.accept()
            print("New connection with a nameserver.")
            self.handle4NameServer(nameserver)
            print("Disconnected from the nameserver.")
            print("Enter [exit] to shut down the dataserver.\n")

    def handle4NameServer(self, nameserver):
        while True:
            command, msg = self.parseCommand(nameserver)
            if command == "1":
                fileName = str(msg, 'utf-8')
                if os.path.isfile(fileName):
                    os.remove(fileName)
                chunkContent, _ = recvAll(nameserver)
                with open(fileName, 'wb') as f:
                    f.write(chunkContent)
                nameserver.sendall("ok".encode())

            elif command == "2": # check a chunk
                chunkName = str(msg, 'utf-8')
                md5, _ = recvAll(nameserver)
                md5 = md5.decode()
                if not os.path.isfile(chunkName):
                    nameserver.sendall(b'0')
                    continue

                with open(chunkName, 'rb') as f:
                    chunkContent = f.read()
                
                if hashlib.md5(chunkContent).hexdigest() != md5:
                    nameserver.sendall(b'0')
                    continue
                nameserver.sendall(b'1'+chunkContent)
            elif command =="3":
                break
    def parseCommand(self, sock):
        data, _ = recvAll(sock)
        return ([str(data[0:1], "utf-8"), data[1:len(data)]])

if __name__ == "__main__":
    dataserver = DataServer()
    dataserver.keepWorking()
