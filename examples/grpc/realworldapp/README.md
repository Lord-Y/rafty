# realworldapp

This is a basic implementation of `rafty` with an `api` that allow us to `CRUD` users.

## Start the cluster

In a shell run:
```bash
go run main.go server --member 127.0.0.1:15051 --member 127.0.0.1:15052
```

In an another shell run:
```bash
go run main.go server --http-port 15081 --grpc-port 15051 --member 127.0.0.1:15050 --member 127.0.0.1:15052
```

In an another shell run:
```bash
go run main.go server --http-port 15082 --grpc-port 15052 --member 127.0.0.1:15050 --member 127.0.0.1:15051
```

Let's see the result:
```bash
# POST
curl 127.0.0.1:15082/api/v1/user -d "firstname=key1&lastname=value2"
# result:
{"message":"OK"}

# GET
key=1 && for i in 80 81 82; do echo $i && curl "127.0.0.1:150$i/api/v1/user/key$key" && echo -e "\n";done
# result:
80
{"firstname":"key1","lastname":"value1"}

81
{"firstname":"key1","lastname":"value1"}

82
{"firstname":"key1","lastname":"value1"}

# DELETE
i=1 && curl -XDELETE "127.0.0.1:15081/api/v1/user/key$i"
# result:
{"message":"OK"}
```

We can do the same with the KV:
```bash

# POST
i=1 && curl 127.0.0.1:15081/api/v1/kv -d "key=key$i&value=value$i"
# result:
{"message":"OK"}

# GET
key=1 && for i in 80 81 82; do echo $i && curl "127.0.0.1:150$i/api/v1/kv/key$key" && echo -e "\n";done
# result:
80
{"key":"key1","value":"value1"}

81
{"key":"key1","value":"value1"}

82
{"key":"key1","value":"value1"}

# DELETE
i=1 && curl -XDELETE "127.0.0.1:15081/api/v1/kv/key$i"
# result:
{"message":"OK"}
```

## Stop the cluster

Just press ctrl+c.