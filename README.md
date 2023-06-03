# Multiplex-Full-duplex Server on Spark Implementation

`Socket Server based on spark architecture`
 
`verseion <v2.2.3>`

## Feature
- MulitPlexing- Non-Blocking Server
- Full-duplex pipe connections on Pool resources handling
- Job Sheduleing


## API
- Find File and Transfer
- Word Count (Dev)


## Acrciteture
- Listen Server
- Cluster Manager
- - Job Scheduling Handler(FIFO)
- - Classifiy Jobs Handler
- - Sub Tasks Handler


## How To Run

- How to compile
- at root dir `./FileC`
`source MakeFile && source MakeClient`

- How to run
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
- `./client/secret.txt`


## Exception Handling
`socket worked with asyncio -non blocking, needed to exception handling`
- timeout :: close socket fd
- Connection failure:: close socket 
- Retires :: close socket
- slow connection :: deal wtih timeout
- partial read :: caused by I/O error or slow network etc.. deal with terminate session

