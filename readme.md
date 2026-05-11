# Smart Redis Connection

## Problem

Redis Sentinel solves master discovery and failover, but most Node.js clients still treat Sentinel mainly as a way to find the current master. That is not enough for this use case.

We need a Redis client wrapper that understands the full Sentinel topology:

- Writes must go to the current master.
- Reads should use healthy replicas when available.
- Reads should fall back to master if replicas are unavailable.
- Master failover should be detected and reconciled automatically.
- Sentinel outages should not stop existing Redis traffic.
- Redis node failures should be handled independently per connection.
- Foreground commands should not be retried automatically, to avoid accidental double writes.

The goal is to keep Redis access stable during topology changes while avoiding reconnect storms, unnecessary full reconnects, and expensive repeated discovery calls.

This library is a small wrapper around `node-redis`. Sentinel is used as the discovery and control plane. Master and replica Redis connections are the data plane.

## Goals

- Support `redis://` direct mode.
- Support `redis+sentinel://` Sentinel mode.
- Discover master and replicas from Sentinel.
- Route writes to master.
- Route reads to healthy replicas.
- Fall back reads to master when replicas are unavailable.
- Reconcile topology changes in the background.
- Listen to Sentinel topology events and debounce reconciliation.
- Heal Sentinel command connections in the background.
- Handle Sentinel, master, and replica failures independently.
- Avoid automatic foreground command retries.

## Non-Goals

- No Redis Cluster support.
- No automatic write retries.
- No dependency-heavy abstraction.
- No hidden `client.get()` / `client.set()` API wrapping.

## Install

Install directly from the public GitHub repository:

```sh
npm install github:kbm-development/redis-sentinel
```

Or with the full Git URL:

```sh
npm install git+https://github.com/kbm-development/redis-sentinel.git
```

## Usage

Create the Redis context once during application startup. `createRedis()` returns a plain context object immediately. Lifecycle functions are functional: they take a context and return the next context.

```js
var { createRedis, connectRedis, command, closeRedis } = require('redis-sentinel')

var redis = createRedis(process.env.REDIS_URL)

var discovered = await redis.ready
console.log(discovered.topology.master)

redis = await connectRedis(discovered)

await command(['SET', 'key', 'value'], redis)
var value = await command(['GET', 'key'], redis)

await closeRedis(redis)
```

Blocking stream readers should use their own Redis context instead of duplicating the shared command client. Clone the discovered context and connect only its master connection:

```js
var { createRedis, connectRedis, connectMasterRedis, cloneRedis } = require('redis-sentinel')

var redis = createRedis(process.env.REDIS_URL)

var discovered = await redis.ready
redis = await connectRedis(discovered)

var streamRedis = cloneRedis(redis)
streamRedis = await connectMasterRedis(streamRedis)

startStreams(streamRedis)
```

The stream reader can keep using `command(args, streamRedis)`. This keeps blocking commands off the shared command connection and avoids depending on `context.master.client.duplicate()` during startup:

```js
await command(['XREADGROUP', 'GROUP', group, consumer, 'BLOCK', '0', 'STREAMS', key, '>'], streamRedis)
```

## Connection Strings

### Direct Redis

```text
redis://host:port
```

### Redis Sentinel

```text
redis+sentinel://host1:26379,host2:26379,host3:26379?sentinelMasterId=mymaster
```

### Redis Sentinel With Auth

```text
redis+sentinel://username:password@host1:26379,host2:26379?sentinelMasterId=mymaster
```

The Sentinel URI points to Sentinel nodes, not Redis master or replica nodes.

## Behavior

- Sentinel discovers the current master and replicas.
- Writes go to the current master.
- Reads go to a healthy replica when available.
- Reads fall back to master if no healthy replica exists.
- Unknown commands are treated as writes and sent to master.
- `createRedis()` starts Sentinel discovery but does not start data-plane Redis connections or background watchers.
- `connectRedis()` connects master/replica data-plane clients and starts background reconciliation/watchers.
- Background reconciliation keeps topology fresh after `connectRedis()`.
- Sentinel pub/sub events trigger debounced reconciliation.
- Sentinel failures do not kill active master/replica connections.
- Failed foreground commands are returned to the caller without automatic retry.

## Lifecycle

- `createRedis(url)` returns a plain context object immediately.
- The returned context has no object methods like `getContext()` or `setContext()`.
- `redis.ready` resolves to a new discovered context with `topology` populated.
- `connectRedis(redis)` returns a new context with shared master/replica clients and background work started.
- `cloneRedis(redis)` creates a separate context from the same discovered topology.
- `connectMasterRedis(clone)` returns a new context with only the clone's master client connected, which is useful for blocking stream readers.
- `command(args, redis)` routes through the shared command connections.
- Blocking stream readers should use a cloned master-only context instead of using `.duplicate()` on `context.master.client`.
- `closeRedis(redis)` closes Redis, Sentinel, background, and subscriber resources owned by that context.

## Deployment Note

Redis replicas may need `replica-announce-ip` configured so Sentinel returns addresses reachable by the Node.js application.

## License

MIT
