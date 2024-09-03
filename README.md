# Horcrux: Implementation of memcached with persitence

## How to build
```bash
cargo build --release
```

## How to start server
```bash
./target/release/horcrux
```

## Example of connecting and disconnecting server
```bash
$ telnet localhost 11211
Trying 127.0.0.1...
Connected to localhost.
Escape character is '^]'.
set harry 0 0 3
8th
STORED
get harry
VALUE harry 0 3
8th
END
quit
Connection closed by foreign host.
```