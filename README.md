# workspace-go
Private workspace for Golang.

# Remote
1. Same as local except that
    1. its locks are global
    1. its tree structure/metadata is always up-to-date
    1. run a rpc server and not not `server.Wait()`

# Procedure
## Control plane
### Master - manager
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
### Client - fbackend, rclient
1. Get (parent) node owner from Master
    1. get whole node object from owner
1. RPC to owner (the one who created it)
    1. read
        1. fetch whole node, client do not store any node or part of node
    1. write
        1. send args, fetch reply
    1. create/delete
        1. send args, fetch reply

### Server - rserver
1. preserve node map `map[uint64]*RNode`
1. check root node
    1. if no root node
        1. local is 1st owner
        1. ask to allocate one
    1. create a local one
1. invoked by `RServer`
1. acquire local lock
    1. read
        1. return whole node
    1. write
        1. process and return reply
    1. create/delete
        1. talk with Master to allocate/deallocate
1. release lock

### TODO
1. shortcut by use local rserver as proxy
    1. if local rserver->fserver has the node
        1. if node is not transfering
            1. reads/write can be handled locally

## Compatibility plane - lfs
1. translate system calls
1. invoke client's methods

# Boot Order
1. Manager (skeleton in network)
1. RServer, on 
    1. FBackEnd, on
        1. RClient (skeleton on node, depending on Manager)
1. LFS

## Design
### Control-Data-Compatibility Model
### Node Transfer
### Node Lock
1. Node locks are inside nodes, for the reduction of consistence complexity.
1. Node has a `canLock` bool, which is set to `true` when created.
1. If a node is to be transferred/destroyed and should not be used anymore, `canLock` is set to false before node id locking, so nobody hangs on it. (Everyone before the switch of `canLock` can read/write since they are in front of the transfer/destroy thread, others will return before trying to lock the node.)
1. In order to handle remote open safely, all remote openers are recorded with their opened handles, which will be closed if the openers failed to send heartbeat back.
1. All opened remote nodes are assigned with a local handle ID, which is used by and unique among local os, and a remote handle ID, which is used by and unique among remote fs. These two IDs can not be same because:
    1. If we use the same IDs, local fs might be assigned with a locally-used ID.
    1. If we ask the master owner to allocate handle Id, even pure local fs access will need a rpc.
    1. If we encode owner ID into handle ID, then we must change the handle ID of the transferring node, which is opened by exactly one opened, since it is transferred and can be transferred.
 1. No file operation needs to manage locks, except for open/create/release, for all other operations are executed after open/create and before close. 

# Plan
1. created child saves locally
1. avoid dead lock: rename lock older node first
1. node lock
1. heartbeat
1. read remote node/remote lock
1. transfer node/write remote node
1. mirror a dir/load dir
1. dump to dir/dump dir
1. save location/multi-client
1. election
1. retry/middle ware
