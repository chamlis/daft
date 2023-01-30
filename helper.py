#!/usr/bin/env python3

import subprocess
import sys

if len(sys.argv) != 3:
    print("Bad args :(")
    sys.exit(1)

cluster_size = int(sys.argv[1])
cluster_index = int(sys.argv[2])

cluster = [f"127.0.0.1:{port}" for port in range(8000, 8000 + cluster_size)]
me = f"127.0.0.1:{8000 + cluster_index}"

command = [
    "go",
    "run",
    ".",
    "-me",
    me,
    "-cluster",
    ",".join(cluster),
    "-state",
    f"{cluster_index}.db"
]

print(" ".join(command))

try:
    subprocess.call(command)
except KeyboardInterrupt:
    pass
