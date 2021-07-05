import hashlib
from socket import *  # 导入 socket 模块
from threading import Thread
DSADDRESS = [
    ('192.168.1.161', 8712),
    ('192.168.209.128', 8712),
    ('192.168.209.129', 8712)    
]
NSADDRESS = ('192.168.1.161', 8765)

class FileObj:
    def __init__(self, fileName:str) -> None:
        self.fileName = fileName
        self.chunkNum = 0
        self.chunkSize = 0
        self.ECLoc = 0
        self.chunkLoc = []
        self.ECMD5 = ""
        self.chunkMD5 = []
        self.lastOffset = 0 

def bxor(b1, b2):
    result = bytearray()
    for b1, b2 in zip(b1, b2):
        result.append(b1^b2)
    return result

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

class NameServer:
    def __init__(self):
        self.fileMap = {}
        self.nameserver = socket(AF_INET, SOCK_STREAM)
        self.nameserver.bind(NSADDRESS)
        self.nameserver.listen(5)
        print("Nameserver construction completed.")
        self.dataserver = [socket() for i in range (3)]
        for i in range(3):
            self.dataserver[i].connect(DSADDRESS[i])
        print("Connected to the dataserver")

    def keepWorking(self):
        t =Thread(target = self.acceptClient, args = ())
        t.setDaemon(True)
        t.start()
        while True:
            s = input("Enter [exit] to shut down the nameserver.\n")
            if s == "exit":
                for i in range(3):
                    self.dataserver[i].sendall(b'3')
                    self.dataserver[i].close()
                break

    def acceptClient(self):
        # 只考虑同一时刻最多处理一个client
        while True:
            client, _ = self.nameserver.accept()
            print("New connection with a client.")
            self.handle4Client(client)
            print("Disconnected from the client.")
            print("Enter [exit] to shut down the nameserver.\n")

    def handle4Client(self, client):
        # 对一个client，处理请求 
        client.sendall("Connected to the nameserver".encode(encoding="utf-8"))
        while True:
            command, msg = self.parseCommand(client)
            if command =="0":
                msg = bytearray()
                msg += (str(len(self.fileMap))+" files in total:\n").encode()
                for k in self.fileMap.keys():
                    msg+=((k+"\n").encode())
                client.sendall(msg)

            elif command == "1":
                fileName = str(msg, 'utf-8')
                if fileName in self.fileMap:
                    client.sendall("Trying to upload an existed file.".encode())
                    continue
                else:
                    self.fileMap[fileName] = FileObj(fileName)
                    client.sendall("ok".encode())

                # receive file from the client
                content, size = recvAll(client)
                chunkSize = size-size//2
                contentPart1 = content[:chunkSize]
                contentPart2 = content[chunkSize:]
                self.fileMap[fileName].chunkNum = 2
                self.fileMap[fileName].chunkSize = chunkSize
                self.fileMap[fileName].chunkLoc = [1, 2]
                self.fileMap[fileName].ECLoc = 0
                self.fileMap[fileName].lastOffset = size//2

                # split the file and generate ec
                barr1, barr2 = [bytearray(chunkSize), bytearray(chunkSize)]
                barr1[:chunkSize] = contentPart1[:]
                barr2[:size//2] = contentPart2[:]
                ec = bxor(barr1, barr2)
                self.fileMap[fileName].chunkMD5 = [
                    hashlib.md5(str(barr1, 'utf-8').encode()).hexdigest(),
                    hashlib.md5(str(barr2, "utf-8").encode()).hexdigest()
                    ]
                self.fileMap[fileName].ECMD5 = hashlib.md5(str(ec, 'utf-8').encode()).hexdigest()
                self.pushFile(fileName, [ec, barr1, barr2])
                client.sendall("ok".encode())

            elif command == "2":
                fileName = str(msg, 'utf-8')
                if fileName not in self.fileMap:
                    client.sendall(("Have not found the file: "+fileName+".").encode())
                else:
                    fileContent, state = self.pullFile(fileName)
                    if state == "ok":
                        client.sendall("ok".encode())
                        client.sendall(fileContent)
                    else:
                        client.sendall("The file is lost or contaminated!".encode())
            elif command == "3":
                break

    def pullFile(self, fileName):
        fobj = self.fileMap[fileName]
        chunks = [bytearray() for _ in range(3)]
        fileContent = bytearray()
        md5 = [fobj.ECMD5]+fobj.chunkMD5
        errID = -1
        isFailed = False
        for i in range(3):
            self.dataserver[i].sendall(("2" + fileName+"_ch"+str(i)).encode())
            self.dataserver[i].sendall(md5[i].encode())
        for i in range(3):
            state, chunks[i] = self.parseCommand(self.dataserver[i])
            if state!="1":
                if errID!=-1:
                    isFailed = True
                else:
                    errID = i
        if isFailed: # more than 1 chunk are lost or contaminated
            del self.fileMap[fileName]
            return fileContent, "error"
        if errID!=-1: # only one chunk is lost or contaminated
            chunks[errID] = bxor(*(chunks[:errID]+chunks[errID+1:]))
            self.pushChunk(fobj.fileName+"_ch"+str(errID), chunks[errID], errID)
        fileContent = chunks[1]+chunks[2][:fobj.lastOffset]
        

        return fileContent, "ok"

    def parseCommand(self, sock):
        data, _ = recvAll(sock)
        return ([str(data[0:1], "utf-8"), data[1:len(data)]])

    def pushFile(self, fileName, chunks):
        for i in range(3):
            self.pushChunk(fileName+"_ch"+str(i), chunks[i], i)

    def pushChunk(self, chunkName, barr, dID):
        self.dataserver[dID].sendall(("1" + chunkName).encode())
        self.dataserver[dID].sendall(barr)
        state = self.dataserver[dID].recv(128).decode(encoding="utf-8")
        if state!="ok":
            print("Failed to send chunk %s to the dataserver" % (chunkName))



if __name__ == "__main__":
    nameserver = NameServer()
    nameserver.keepWorking()