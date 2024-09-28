# Contributing

## Linter

In order to contribute to our project, you need to make sure to install [golangci-lint](https://golangci-lint.run/usage/install/#binaries).

Then, configure git hooks:
```bash
git config core.hooksPath .githooks
```

## MacOS loopback

When trying to run `go run main.go` on MacOS from `examples/grpc/cluster-3-nodes/serverX` directory, you may encounter the error `Fail to serve gRPC server error="Fail to listen gRPC server: listen tcp 127.0.0.2:50052: bind: can't assign requested address" logProvider=rafty`.

The solution is to add the new loopback ips:
```bash
for i in $(seq 2 10); do sudo ifconfig lo0 alias 127.0.0.$i;done
```
To delete them:
```bash
for i in $(seq 2 10); do sudo ifconfig lo0 delete 127.0.0.$i;done

```
