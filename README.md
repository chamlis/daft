# Daft

*NOUN*: blend of "dumb" and "[Raft](https://raft.github.io)"

*ADJECTIVE*: [someone](https://github.com/chamlis) who thinks they
have a hope of writing a functional consensus implementation in one
evening

This was hastily written over a period of one month for some
coursework, please **do not** use this for anything that matters.

Run the demo application, a simple arithmetic accumulator with
commands for incrementing and tripling, on a local cluster with three
clients and Raft nodes, either manually with:

- `go run . -cluster 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 -me
  127.0.0.1:8000 -state 0.db`
- `go run . -cluster 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 -me
  127.0.0.1:8001 -state 1.db`
- `go run . -cluster 127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002 -me
  127.0.0.1:8002 -state 2.db`

or using the helper Python script with:

- `./helper.py 3 0`
- `./helper.py 3 1`
- `./helper.py 3 2`