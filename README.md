# workspace-go
Private workspace for Golang.

# Remote
1. Same as local except that
    1. its locks are global
    1. its tree structure/metadata is always up-to-date
    1. run a rpc server and not not `server.Wait()`

# Procedure
1. check local lock
    1. if not acquired: acquire remote lock and modify time
1. if read/write/get:
    1. check local cache (Inode.data, Inode.children, etc)
        1. If not latest: fetch remote
1. process
1. sync metadate (times, location) to remote
1. release lock

# Plan
1. create/delete
1. server use Inode
1. flush, fsync, etc
1. read/write lock
1. mirror a dir
1. dump to dir
1. save location/multi-client
1. election

# TODO
1. mirror a dir
