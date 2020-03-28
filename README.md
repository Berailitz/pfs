# workspace-go
Private workspace for Golang.

# Remote
1. Same as local except that
    1. its locks are global
    1. its tree structure/metadata is always up-to-date
    1. run a rpc server and not not `server.Wait()`

# Procedure
## Control plane
### Master
1. preserve owner table `map[uint64]OwnerID`, `map[OwnerID]OwnerAddress`
1. manager node ids
    1. create/allocate
        1. next++, never recycle ids
    1. delete/deallocate
        1. remove from owner table
    1. transfer node (and child nodes) between owners
        1. src -> Master: mark node as transferring
            1. lock node node
            1. update transferring node table
        1. src -> dst: transfer data
        1. dst -> Master: mark node as transferred
            1. update owner table
            1. unlock node
            1. dst remove node

## Data plane
### Client
1. Get (parent) node owner from Master
    1. get whole node object from owner
1. RPC to owner (the one who created it)
    1. read
        1. fetch whole node, client do not store any node or part of node
    1. write
        1. send args, fetch reply
    1. create/delete
        1. send args, fetch reply

### Server
1. preserve node map `map[uint64]*RNode`
1. invoked by `RServer`
1. acquire local lock
    1. read
        1. return whole node
    1. write
        1. process and return reply
    1. create/delete
        1. talk with Master to allocate/deallocate
1. release lock

## Compatibility plane
1. translate system calls
1. invoke client's methods

# Plan
1. rpc proto
1. rpc client
1. rpc server
1. always run rpc server
1. master flag
1. remote lock
1. read/write lock
1. mirror a dir
1. dump to dir
1. save location/multi-client
1. election

# TODO
1. mirror a dir
