# Multiplex-Full-duplex Server on Spark Implementation

`File Transfer Server based on spark architecture`
`verseion <v2.2.0>`

## Feature
- MulitPlexing-Duplexing Server with epoll and n-full-duplex pipe
- virtualized network and process
- Job Sheduleing at server cluster with FIFO
- Can Handle with multi-session


## api
- Find File with non-blocking I/O
- Transfer File
 

## Acrciteture
- Listen Server with Handler
- Cluster Manager
⋅ - Job Scheduling(FIFO)

## How To Run

- how to compile
- at root dir `./FileC`
`source MakeFile && source MakeClient`

- how to run
</B>Server
```shell
./start_server

```

</B>Client
```shell
./start_client
```

IMPORTANT
- SECRET NOT INCLUDED IN GIT, WRITE SECRET ON YOUR CONTROL.TXT
- `./server/control.txt'`

