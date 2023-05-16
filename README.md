# Filec
`File Transfer Server based on spark architecture`

## Feature
- have blocking-epoll Listen server
- virtualized network and process
- Job Sheduleing at server cluster with FIFO
- have  Multi-Plexing feature for processing multi-session
- worked on (thread-pool & ) main-cluster and  child worker thread


## api
- Find File with non-blocking I/O
- Transfer File
 

## Acrciteture
`There is 3 Sub-Thread for each middle-ware server`
- pair-wise Pipe connection with thread-pool (with no lock wait)
- Listen Server
- Job Scheduler(Just FIFO)

## How To Run

- how to compile
- at root dir `./FileC`
`source MakeFile && source MakeClient`

- how to run
serveri(get your control.file at server/control)
```shell
./start_server

 ```
client
```shell
./start_client
```

if you have your own specific inet and port,
modified `./server/control.txt'
`

