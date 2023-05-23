# Filec
`File Transfer Server based on spark architecture`

## Feature
- MulitPlexing-Duplexing Server with epoll and n-full-duplex pipe
- virtualized network and process
- Job Sheduleing at server cluster with FIFO
- Can Handle with multi-session


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

