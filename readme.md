## Problem

we are currently at the stage of needed redis library thats smart enough to handle connections changes, after looking at library out there, non of statisfied our need, usual redis sentinel is always connect to master which is we wanted to balancing load from slave as well, so we need smart routing the commands what goes to master what goes to slave, we also need to make no down time when master is down, by sentinel default it would promote in the background new master, and our clients library will re-establish it topology reconnect without glitch in the client, so we need to reconnect, if master,slave, or sentinel dies, it known and smart enough to handle the changes


We want to build a small Node.js Redis client wrapper that supports Redis Sentinel discovery, read/write routing, topology changes, failover recovery, and low-disruption connection management.

Create a Redis Sentinel aware library using `node-redis` with these behaviors:

- Support normal `redis://` direct mode.
- Support `redis+sentinel://` discovery mode.
- Discover master and replicas from Sentinel.
- Route writes to master.
- Route reads to replicas when available.
- Fall back reads to master when no healthy replica exists.
- Detect Sentinel/topology changes without reconnect storms.
- Handle master, replica, and Sentinel failures independently.
- Avoid automatic command retry that could cause accidental double writes.
- Keep implementation functional, small, and dependency-light.


## Connection String Shape

Direct Redis mode:

```text
redis://host:port
```

Sentinel mode:

```text
redis+sentinel://host1:26379,host2:26379,host3:26379?sentinelMasterId=mymaster
```

Optional auth form:

```text
redis+sentinel://username:password@host1:26379,host2:26379?sentinelMasterId=mymaster
```

The Sentinel URI points to Sentinel nodes, not Redis master or replica nodes.

## Target State Shape

The context should eventually contain:

- `mode`: `direct` or `sentinel`
- `sentinels`: configured Sentinel candidates
- `sentinel`: currently active Sentinel command connection
- `sentinelSubscriber`: active Sentinel event subscription connection
- `masterName`: Sentinel master name
- `username` / `password`: Redis auth options
- `master`: current master connection record
- `replicas`: current replica connection records
- `topology`: last known topology snapshot
- `timers`: background reconciliation / health timers
- `options`: intervals, thresholds, backoff config

Each Redis node connection record should eventually contain:

- `key`: stable `host:port` key
- `role`: `master` or `replica`
- `host`
- `port`
- `client`
- `status`: `up`, `suspect`, `down`, `connecting`, or `closed`
- `failures`
- `retry`: per-connection retry state

