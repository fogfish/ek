# Erlang Kluster

Distributed process groups, topology management and routing using consistent hashing.

## Inspiration

The library is looking after Erlang's `pg2`. It avoids the usage of global locks 
for group state management and provides alternative approaches to address processes. 

## Key features

* distributed process group(s) with notification on change of topology
* key routing using consistent hashing
* uses CRDTs OR-Set to reconcile state of distributed group


