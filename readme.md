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

Create the Redis handle once during application startup. The handle can be used immediately; commands wait for the initial connection to finish.

```js
var { createRedis, command, closeRedis } = require('redis-sentinel')

var redis = createRedis(process.env.REDIS_URL)

await command(['SET', 'key', 'value'], redis)
var value = await command(['GET', 'key'], redis)

await closeRedis(redis)
```

If the application prefers explicit startup sequencing, `createRedis()` can still be awaited:

```js
var redis = await createRedis(process.env.REDIS_URL)
```

For manual connection control, disable auto-connect and call `connect()` yourself:

```js
var redis = createRedis(process.env.REDIS_URL, { autoConnect: false })

await redis.connect()
await command(['PING'], redis)
await closeRedis(redis)
```

If code needs to duplicate the active master client directly, for example a blocking stream reader, wait for Redis readiness before reading `context.master.client`:

```js
var redis = createRedis(process.env.REDIS_URL)

await redis.connect()
startStreams(redis)
```

Normal `command()` calls do this wait internally. Direct client duplication cannot, unless the stream helper explicitly waits first.

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
- Background reconciliation keeps topology fresh.
- Sentinel pub/sub events trigger debounced reconciliation.
- Sentinel failures do not kill active master/replica connections.
- `command()` waits for the initial connection when given a newly created Redis handle.
- Failed foreground commands are returned to the caller without automatic retry.

## Lifecycle

- `createRedis(url)` returns a Redis handle and starts connecting in the background.
- `await createRedis(url)` waits for the initial connection before continuing.
- `createRedis(url, { autoConnect: false })` creates a handle without opening sockets until `await redis.connect()`.
- Stream readers that duplicate `context.master.client` should start after `await redis.connect()` or should perform the same readiness wait inside the reader helper.
- `closeRedis(redis)` waits for any in-progress connection attempt and then closes Redis, Sentinel, background, and subscriber resources.

## Deployment Note

Redis replicas may need `replica-announce-ip` configured so Sentinel returns addresses reachable by the Node.js application.

## License

MIT
