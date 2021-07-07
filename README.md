# Toy-XOR-Erasure-Coding

Project of CS7316: Large-scale Data Processing Technology [2021 Spring].

In this project, I just implement a naive xor-ec not for Hadoop or Minio but for my own toy distributed file system... :)

## Instructions:

To run the source code with a minimal example:

for the data server:

```bash
cd dataserver
python dataserver.py
```

for the name server:

```bash
cd nameserver
python nameserver.py
```

for the client:

```bash
cd client
python client.py
```

1. Type the command on the client interface to upload files. 
2. Manually modify the chunk stored in the data server.
3. Type the command on the client interface to get files back. 
