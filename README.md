# rfs

a fuse network filesystem

this network filesystem based on GRPC and fuse:
- client use GRPC to communicate with server
- client mount filesystem through linux fuse3

## Usage

### server
```
rfs-server 0.3.0
rfs server.

USAGE:
    rfs-server [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -c, --config <config>     [default: /etc/rfs/server.yml]
```

### server config
```yaml
root_path: /tmp/test
listen_addr: "127.0.0.1:9876"
cert_path: cert.pem
key_path: key.pem
ca_path: ca.pem
debug: true
```

### client
```
rfs-client 0.3.0
rfs client.

USAGE:
    rfs-client [OPTIONS]

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -c, --config <config>     [default: /etc/rfs/client.yml]
```

### client config
```yaml
mount_path: /tmp/client-test
server_addr: "https://rfs-server:9876"
cert_path: cert.pem
key_path: key.pem
debug_ca_path: ca.pem # only use for debug, DO NOT use in production.
debug: true
```

## License

MIT
