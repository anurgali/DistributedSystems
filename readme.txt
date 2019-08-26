The DFS is designed as a globally centralized system.
There are three types of nodes: 
1) client - queries the server
2) master - Name Node analog from HDFS
3) slave - the actual data node.===

Client sends all requests to Master, which determines from the hash value of the path
which slave is supposed to store the specified path. The master uses a lookup
tree to quickly find the slave. Once the slave has been identified the master sends
the request to that slave and optionally waits for reply, this reply is resent to client.
The DFS follows "write once, read many" approach - i.e. no file editing is supported.

Supported commands:
init - initialize the client and register it in slaves
fread [path] - read contents of the file
fwrite [path] [message] - write into the file
fdel [path] - delete the file
finfo - get the file info
cd - change the current directory
ls [path] - view the contents of the directory, path is optional
mkdir - make directory
rmdir - remove directory
exit - exit from system

To launch master:
java -jar dfs.jar -m [ip]:[port] [slaves file] [log.file]

for example,
java -jar dfs.jar -m localhost:50000 slaves.txt master.txt

To launch client:
java -jar dfs.jar -c [ip]:[port] [master ip]:[master port] [log.file]

for example,
java -jar dfs.jar -c localhost:50002 localhost:50000 client.txt

To launch client:
java -jar dfs.jar -s [ip]:[port]

for example,
java -jar dfs.jar -s localhost:50001
