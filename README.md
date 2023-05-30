# Multiplex-Full-duplex Server on Spark Implementation

`File Transfer Server based on spark architecture`
`verseion <v2.2.0>`

## Feature
- MulitPlexing- Non-Blocking Server
- Full-duplex pipe connections on Pool resources handling
- Job Sheduleing at server cluster with FIFO
- Virtualized network and process


## API
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
- `./client/secret.txt`

### discrition

- seting into Asyncio or worked on Multiplexing is not same as epoll
- there is many cases to deal with connections failure, partial read, retires and peer network speed

- current version is having some partial edge -cases events
⋅ - connection failure cases
⋅ - Timeout cases
