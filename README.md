# Distributed File System (DFS)

## Description
This is a distributed file system with a multithreaded server (`dfs.c`) and client (`dfc.c`) in C. Files are chunked and distributed across servers using MD5 hash for load balancing, with redundancy (each chunk on two servers). Supports put, get, and list operations, handling server availability and version conflicts via timestamps.

## Features
- **Chunking & Distribution**: Files split into chunks based on available servers; hash determines placement.
- **Redundancy**: Each chunk stored on two servers for fault tolerance.
- **Operations**: put [file], get [file], list [file] or list all.
- **Server Availability**: Client checks and skips unavailable servers.
- **Versioning**: Get prefers latest timestamp; if tie, fewer chunks.
- **Concurrency**: Server uses threads for multiple clients.
- **Config**: Client uses `dfc.conf` for server details.

## Assumptions
- List: Prints "<filename> not found" if absent; "No files found..." if none match.
- Put: Prints success if uploaded; requires at least 2 servers.
- Get: Reconstructs from latest/complete version; prints success or incomplete.
- If multiple versions, prefers latest timestamp, then fewer chunks (no logic for exact ties).
- File names <200 chars; max 10 servers.
- Client name: "client<PID><random_str>" for uniqueness.

## Compilation
```bash
gcc dfs.c -o dfs_server -lpthread
gcc dfc.c -o dfs_client
```

## Usage
- **Server**: Run on each machine with directory and port.
  ```bash
  ./dfs_server <dir> <port>
  ```
  Example:
  ```bash
  ./dfs_server ./server1 8001
  ```

- **Client**: Use with dfc.conf in home dir; supports multiple files.
  ```bash
  ./dfs_client <command> [filename] ...
  ```
  Example:
  ```bash
  ./dfs_client put example.txt
  ./dfs_client get example.txt
  ./dfs_client list
  ```

## dfc.conf Example
```
server DFS1 127.0.0.1:8001
server DFS2 127.0.0.1:8002
```

## Dependencies
- POSIX threads (`-lpthread`).

## Notes
- Debug: Set `#define DEBUG 1` for logs.
- Servers create dir if missing; clients use home for conf.
