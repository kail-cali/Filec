# Filec
`File Transfer Server based on spark architecture`

## Feature
- have Non-blocking Listen server
- virtualized network and process
- Job Sheduleing at server cluster
- have  Multi-Plexing feature for processing multi-session
- worked on (thread-pool & ascio) main-cluster and  child worker thread


## api
- Find File with non-blocking I/O
- Transfer File
 

## Acrciteture
`There is 3 Sub-Thread for each middle-ware server`
- Listen Server
- Pipe for multi-Worker thread 
- Job Scheduler 

## How To Run
- how to compile
- at root dir `./FileC`
`source MakeFile && source MakeClient`
- how to run
server
`./start_server`

and client
`./start_client`

if you have your own specific inet and port,
modified `./server/control.txt'
`

