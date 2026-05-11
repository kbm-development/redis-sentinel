'use strict'

var redis = require('redis')

var SENTINEL_PROTOCOL = 'redis+sentinel://'

var SENTINEL_TOPOLOGY_EVENTS = new Set([
    '+switch-master',
    '+slave',
    '+sdown',
    '-sdown',
    '+odown',
    '+failover-end'
])

var READ_COMMANDS = new Set([
    'GET', 'MGET', 'GETRANGE', 'STRLEN',
    'EXISTS', 'TTL', 'PTTL',
    'KEYS', 'SCAN',
    'LRANGE', 'LINDEX', 'LLEN',
    'SMEMBERS', 'SCARD', 'SISMEMBER',
    'ZRANGE', 'ZREVRANGE',
    'ZSCORE', 'ZCARD',
    'ZRANK', 'ZREVRANK',
    'HGET', 'HGETALL', 'HKEYS',
    'HVALS', 'HLEN', 'HEXISTS',
    'XRANGE', 'XREVRANGE',
    'XREAD', 'XINFO',
    'JSON.GET', 'JSON.MGET',
    'FT.SEARCH', 'FT.AGGREGATE',
    'GRAPH.RO_QUERY',
    'TS.GET', 'TS.MGET', 'TS.RANGE', 'TS.MRANGE'
])

var WRITE_COMMANDS = new Set([
    'SET', 'SETNX', 'SETEX', 'PSETEX', 'APPEND',
    'INCR', 'DECR', 'INCRBY', 'DECRBY', 'INCRBYFLOAT',
    'MSET', 'MSETNX', 'SETBIT', 'SETRANGE',
    'DEL', 'UNLINK', 'EXPIRE', 'EXPIREAT', 'PEXPIRE', 'PEXPIREAT',
    'PERSIST', 'RENAME', 'RENAMENX', 'MOVE', 'COPY',
    'RESTORE', 'MIGRATE',
    'RPUSH', 'LPUSH', 'RPUSHX', 'LPUSHX',
    'LINSERT', 'LSET', 'LTRIM',
    'RPOP', 'LPOP', 'RPOPLPUSH',
    'LMOVE', 'BLMOVE', 'LREM',
    'SADD', 'SREM', 'SMOVE', 'SPOP',
    'SINTERSTORE', 'SUNIONSTORE', 'SDIFFSTORE',
    'ZADD', 'ZINCRBY', 'ZREM',
    'ZREMRANGEBYRANK',
    'ZREMRANGEBYSCORE',
    'ZREMRANGEBYLEX',
    'ZUNIONSTORE',
    'ZINTERSTORE',
    'BZPOPMIN', 'BZPOPMAX',
    'ZPOPMIN', 'ZPOPMAX',
    'HSET', 'HSETNX', 'HMSET',
    'HINCRBY', 'HINCRBYFLOAT', 'HDEL',
    'XADD', 'XDEL', 'XTRIM',
    'XREADGROUP',
    'XGROUP', 'XSETID',
    'XACK', 'XAUTOCLAIM', 'XCLAIM',
    'PUBLISH',
    'MULTI', 'EXEC', 'DISCARD', 'WATCH', 'UNWATCH',
    'JSON.SET', 'JSON.MSET', 'JSON.DEL',
    'JSON.NUMINCRBY', 'JSON.NUMMULTBY',
    'JSON.STRAPPEND',
    'JSON.ARRAPPEND',
    'JSON.ARRINSERT',
    'JSON.ARRPOP',
    'JSON.ARRTRIM',
    'JSON.CLEAR',
    'FT.CREATE', 'FT.ALTER',
    'FT.DROPINDEX',
    'FT.ALIASADD', 'FT.ALIASDEL',
    'FT.SUGADD', 'FT.SUGDEL',
    'GRAPH.DELETE', 'GRAPH.QUERY',
    'TS.CREATE', 'TS.ALTER',
    'TS.ADD', 'TS.MADD',
    'TS.INCRBY', 'TS.DECRBY',
    'TS.CREATERULE', 'TS.DELETERULE',
    'EVAL', 'EVALSHA',
    'FUNCTION', 'SCRIPT',
    'FLUSHALL', 'FLUSHDB',
    'CONFIG', 'MODULE',
    'ACL', 'CLIENT'
])

var assertString = (value, name) => {
    if (typeof value !== 'string' || value.trim() === '') {
        throw new Error(`${name} must be a non-empty string`)
    }
}

var parsePort = (value, defaultPort, label) => {
    if (value == null || value === '') {
        if (defaultPort == null) throw new Error(`${label} port must be a valid TCP port`)
        return defaultPort
    }

    var port = Number(value)
    if (!Number.isInteger(port) || port <= 0 || port > 65535) {
        throw new Error(`${label} port must be a valid TCP port`)
    }

    return port
}

var parseAuth = (auth) => {
    if (!auth) return { username: undefined, password: undefined }

    var parts = auth.split(':')
    var username = decodeURIComponent(parts.shift() || '')
    var password = decodeURIComponent(parts.join(':') || '')

    return {
        username: username || undefined,
        password: password || undefined
    }
}

var parseSentinelNode = (node) => {
    var clean = node.trim()
    if (!clean) throw new Error('sentinel host must be a non-empty string')

    var parts = clean.split(':')
    var host = parts[0]
    var port = parsePort(parts[1], 26379, 'sentinel')

    if (!host) throw new Error('sentinel host must be a non-empty string')

    return Object.freeze({ host, port })
}

var parseSentinelUrl = (uri) => {
    var clean = uri.slice(SENTINEL_PROTOCOL.length)
    var [authAndHosts, query = ''] = clean.split('?')
    var auth = undefined
    var hosts = authAndHosts

    if (authAndHosts.includes('@')) {
        var authSplit = authAndHosts.split('@')
        auth = authSplit.shift()
        hosts = authSplit.join('@')
    }

    if (!hosts) throw new Error('redis sentinel url must include at least one sentinel host')

    var sentinels = hosts.split(',').map(parseSentinelNode)
    var params = new URLSearchParams(query)
    var masterName = params.get('sentinelMasterId')
    var parsedAuth = parseAuth(auth)

    if (!sentinels.length) throw new Error('redis sentinel url must include at least one sentinel host')
    if (!masterName) throw new Error('redis sentinel url must include sentinelMasterId')

    return Object.freeze({
        mode: 'sentinel',
        uri,
        sentinels: Object.freeze(sentinels),
        masterName,
        username: parsedAuth.username,
        password: parsedAuth.password
    })
}

var parseDirectUrl = (uri) => {
    return Object.freeze({
        mode: 'direct',
        uri,
        sentinels: Object.freeze([]),
        masterName: undefined,
        username: undefined,
        password: undefined
    })
}

var parseRedisUrl = (uri) => {
    assertString(uri, 'redis url')

    if (uri.startsWith(SENTINEL_PROTOCOL)) return parseSentinelUrl(uri)

    return parseDirectUrl(uri)
}

var createInitialContext = (uri, options = {}) => {
    var config = parseRedisUrl(uri)

    return Object.freeze({
        ...config,
        sentinel: undefined,
        sentinelSubscriber: undefined,
        master: undefined,
        replicas: Object.freeze([]),
        topology: undefined,
        timers: Object.freeze({}),
        options: Object.freeze({ ...options })
    })
}

var createRedisClient = (options) => redis.createClient(options)

var createConnectionRecord = (role, key, client, node = {}, status = 'up') => {
    return Object.freeze({
        key,
        role,
        host: node.host,
        port: node.port,
        client,
        status,
        failures: 0,
        retry: Object.freeze({
            attempt: 0,
            delay: 0,
            next: 0
        })
    })
}

var getHealthOptions = (options = {}) => Object.freeze({
    failureThreshold: options.failureThreshold || 2,
    baseDelay: options.baseDelay || 100,
    maxDelay: options.maxDelay || 5000,
    now: options.now || Date.now()
})

var createRetryState = (attempt = 0, delay = 0, next = 0) => Object.freeze({
    attempt,
    delay,
    next
})

var getBackoffDelay = (attempt, options = {}) => {
    var healthOptions = getHealthOptions(options)
    var delay = healthOptions.baseDelay * Math.pow(2, Math.max(0, attempt - 1))

    return Math.min(delay, healthOptions.maxDelay)
}

var nextRetryState = (retry = {}, options = {}) => {
    var healthOptions = getHealthOptions(options)
    var attempt = (retry.attempt || 0) + 1
    var delay = getBackoffDelay(attempt, healthOptions)

    return createRetryState(attempt, delay, healthOptions.now + delay)
}

var markConnectionFailure = (connection, options = {}) => {
    var healthOptions = getHealthOptions(options)
    var failures = (connection.failures || 0) + 1
    var status = failures >= healthOptions.failureThreshold ? 'down' : 'suspect'

    return Object.freeze({
        ...connection,
        status,
        failures,
        retry: nextRetryState(connection.retry, healthOptions)
    })
}

var markConnectionSuccess = (connection) => {
    return Object.freeze({
        ...connection,
        status: 'up',
        failures: 0,
        retry: createRetryState()
    })
}

var canRetryNow = (connection, now = Date.now()) => {
    return !connection.retry || !connection.retry.next || connection.retry.next <= now
}

var createClientHealthRecord = (role, client, status = 'up') => createConnectionRecord(role, role, client, {}, status)

var getContextHealthRecord = (context, target) => {
    if (target === 'master') return context.master
    if (target === 'sentinel') return context.sentinelHealth || createClientHealthRecord('sentinel', context.sentinel)
    if (target === 'sentinelSubscriber') return context.sentinelSubscriberHealth || createClientHealthRecord('sentinelSubscriber', context.sentinelSubscriber)
    if (target && target.replicaKey) return (context.replicas || []).find(replica => replica.key === target.replicaKey)
    return undefined
}

var setContextHealthRecord = (context, target, record) => {
    if (target === 'master') return Object.freeze({ ...context, master: record })
    if (target === 'sentinel') return Object.freeze({ ...context, sentinelHealth: record })
    if (target === 'sentinelSubscriber') return Object.freeze({ ...context, sentinelSubscriberHealth: record })
    if (target && target.replicaKey) {
        return Object.freeze({
            ...context,
            replicas: Object.freeze((context.replicas || []).map(replica => {
                if (replica.key === target.replicaKey) return record
                return replica
            }))
        })
    }

    throw new Error('unknown health target')
}

var markContextConnectionFailure = (context, target, options = {}) => {
    var record = getContextHealthRecord(context, target)
    if (!record) throw new Error('health target not found')

    return setContextHealthRecord(context, target, markConnectionFailure(record, options))
}

var markContextConnectionSuccess = (context, target) => {
    var record = getContextHealthRecord(context, target)
    if (!record) throw new Error('health target not found')

    return setContextHealthRecord(context, target, markConnectionSuccess(record))
}

var createDownConnectionRecord = (role, node) => {
    return Object.freeze({
        key: node.key,
        role,
        host: node.host,
        port: node.port,
        client: undefined,
        status: 'down',
        failures: 1,
        retry: Object.freeze({
            attempt: 1,
            delay: 0,
            next: 0
        })
    })
}

var createRedisNodeClientOptions = (node, context) => {
    return {
        socket: {
            host: node.host,
            port: node.port
        },
        username: context.username,
        password: context.password
    }
}

var connectRedisNode = async (role, node, context, createClient = createRedisClient) => {
    var client = createClient(createRedisNodeClientOptions(node, context))

    try {
        await client.connect()
        return createConnectionRecord(role, node.key, client, node)
    } catch (err) {
        await closeClient(client)
        throw err
    }
}

var connectDirect = async (context, createClient = createRedisClient) => {
    if (!context || context.mode !== 'direct') {
        throw new Error('connectDirect requires a direct redis context')
    }

    var client = createClient({ url: context.uri })
    await client.connect()

    return Object.freeze({
        ...context,
        master: createConnectionRecord('master', context.uri, client)
    })
}

var createSentinelClientOptions = (sentinel, context) => {
    return {
        socket: {
            host: sentinel.host,
            port: sentinel.port
        },
        username: context.username,
        password: context.password
    }
}

var connectSentinel = async (context, createClient = createRedisClient) => {
    if (!context || context.mode !== 'sentinel') {
        throw new Error('connectSentinel requires a sentinel redis context')
    }

    var lastError = undefined

    for (var i = 0; i < context.sentinels.length; i += 1) {
        var sentinel = context.sentinels[i]
        var client = createClient(createSentinelClientOptions(sentinel, context))

        try {
            await client.connect()

            return Object.freeze({
                ...context,
                sentinel: client,
                sentinelIndex: i,
                sentinelHealth: createClientHealthRecord('sentinel', client)
            })
        } catch (err) {
            lastError = err
            await closeClient(client)
        }
    }

    var error = new Error('Cannot connect sentinel, no sentinel available')
    error.cause = lastError
    throw error
}

var createSentinelKey = (sentinel) => createNodeKey(sentinel.host, sentinel.port)

var getSentinelHealthMap = (context) => context.sentinelCandidateHealth || Object.freeze({})

var getSentinelCandidateHealth = (context, sentinel) => {
    var key = createSentinelKey(sentinel)
    return getSentinelHealthMap(context)[key] || createConnectionRecord('sentinelCandidate', key, undefined, sentinel)
}

var setSentinelCandidateHealth = (context, sentinel, record) => {
    var key = createSentinelKey(sentinel)

    return Object.freeze({
        ...context,
        sentinelCandidateHealth: Object.freeze({
            ...getSentinelHealthMap(context),
            [key]: record
        })
    })
}

var markSentinelCandidateFailure = (context, sentinel, options = {}) => {
    var health = getSentinelCandidateHealth(context, sentinel)
    return setSentinelCandidateHealth(context, sentinel, markConnectionFailure(health, options))
}

var markSentinelCandidateSuccess = (context, sentinel, client) => {
    var key = createSentinelKey(sentinel)
    var health = createConnectionRecord('sentinelCandidate', key, client, sentinel)
    return setSentinelCandidateHealth(context, sentinel, health)
}

var getNextSentinelIndexes = (context) => {
    var total = context.sentinels.length
    var start = ((context.sentinelIndex || 0) + 1) % total
    var indexes = []

    for (var offset = 0; offset < total; offset += 1) {
        indexes.push((start + offset) % total)
    }

    return indexes
}

var canTrySentinelCandidate = (context, sentinel, now = Date.now()) => {
    return canRetryNow(getSentinelCandidateHealth(context, sentinel), now)
}

var connectSentinelCandidate = async (context, sentinel, createClient = createRedisClient) => {
    var client = createClient(createSentinelClientOptions(sentinel, context))

    try {
        await client.connect()
        return client
    } catch (err) {
        await closeClient(client)
        throw err
    }
}

var healSentinelOnce = async (context, options = {}) => {
    if (!context || context.mode !== 'sentinel') {
        throw new Error('healSentinelOnce requires a sentinel redis context')
    }

    var createClient = options.createClient || createRedisClient
    var dataCreateClient = options.dataCreateClient || createRedisClient
    var healthOptions = getHealthOptions(options)
    var indexes = getNextSentinelIndexes(context)
    var nextContext = context

    for (var index of indexes) {
        var sentinel = context.sentinels[index]
        if (!canTrySentinelCandidate(nextContext, sentinel, healthOptions.now)) continue

        try {
            var client = await connectSentinelCandidate(nextContext, sentinel, createClient)
            var oldSentinel = nextContext.sentinel
            nextContext = markSentinelCandidateSuccess(nextContext, sentinel, client)
            nextContext = Object.freeze({
                ...nextContext,
                sentinel: client,
                sentinelIndex: index,
                sentinelHealth: createClientHealthRecord('sentinel', client),
                sentinelStatus: 'up'
            })

            if (oldSentinel && oldSentinel !== client) await closeClient(oldSentinel)

            return reconcileTopology(nextContext, dataCreateClient)
        } catch (err) {
            nextContext = markSentinelCandidateFailure(nextContext, sentinel, healthOptions)
        }
    }

    return markSentinelSuspect(nextContext, new Error('Cannot heal sentinel, no sentinel available'))
}

var getSentinelHealInterval = (context, options = {}) => {
    return options.intervalMs || context.options.sentinelHealIntervalMs || 1000
}

var createSentinelHealer = (context, options = {}) => {
    var current = context
    var running = false
    var scheduler = options.scheduler || createScheduler()
    var intervalMs = getSentinelHealInterval(context, options)
    var heal = async () => {
        if (running) return current

        running = true
        try {
            current = await healSentinelOnce(current, options)
            return current
        } finally {
            running = false
        }
    }
    var timer = scheduler.setInterval(() => {
        heal().catch(() => undefined)
    }, intervalMs)

    return Object.freeze({
        getContext: () => current,
        heal,
        stop: () => scheduler.clearInterval(timer),
        timer,
        intervalMs
    })
}

var createNodeKey = (host, port) => `${host}:${port}`

var normalizeMaster = (reply) => {
    if (!Array.isArray(reply) || reply.length < 2) {
        throw new Error('invalid sentinel master response')
    }

    var host = reply[0]
    var port = parsePort(reply[1], undefined, 'master')

    if (!host) throw new Error('invalid sentinel master host')

    return Object.freeze({
        host,
        port,
        key: createNodeKey(host, port)
    })
}

var sentinelArrayToObject = (values) => {
    var result = {}

    for (var i = 0; i < values.length; i += 2) {
        result[values[i]] = values[i + 1]
    }

    return result
}

var isReplicaDown = (replica) => {
    var flags = String(replica.flags || '')
    return flags.split(',').includes('s_down') || flags.split(',').includes('o_down')
}

var normalizeReplica = (reply) => {
    var replica = Array.isArray(reply) ? sentinelArrayToObject(reply) : reply
    var host = replica.ip || replica.host
    var port = parsePort(replica.port, undefined, 'replica')

    if (!host) throw new Error('invalid sentinel replica host')

    return Object.freeze({
        host,
        port,
        key: createNodeKey(host, port)
    })
}

var normalizeReplicas = (reply) => {
    if (!Array.isArray(reply)) throw new Error('invalid sentinel replicas response')

    return Object.freeze(reply
        .map(item => Array.isArray(item) ? sentinelArrayToObject(item) : item)
        .filter(item => !isReplicaDown(item))
        .map(normalizeReplica))
}

var getSentinelClient = (context) => {
    if (!context || context.mode !== 'sentinel') {
        throw new Error('sentinel discovery requires a sentinel redis context')
    }

    if (!context.sentinel) {
        throw new Error('sentinel redis context is not connected')
    }

    return context.sentinel
}

var discoverMaster = async (context) => {
    var client = getSentinelClient(context)
    var reply = await client.sendCommand([
        'SENTINEL',
        'get-master-addr-by-name',
        context.masterName
    ])

    return normalizeMaster(reply)
}

var discoverReplicas = async (context) => {
    var client = getSentinelClient(context)
    var reply = await client.sendCommand([
        'SENTINEL',
        'replicas',
        context.masterName
    ])

    return normalizeReplicas(reply)
}

var discoverTopology = async (context) => {
    var master = await discoverMaster(context)
    var replicas = await discoverReplicas(context)
    var topology = Object.freeze({ master, replicas })

    return Object.freeze({
        ...context,
        topology
    })
}

var connectReplicaNode = async (node, context, createClient) => {
    try {
        return await connectRedisNode('replica', node, context, createClient)
    } catch (err) {
        return createDownConnectionRecord('replica', node)
    }
}

var connectTopology = async (context, createClient = createRedisClient) => {
    if (!context || context.mode !== 'sentinel') {
        throw new Error('connectTopology requires a sentinel redis context')
    }

    if (!context.topology || !context.topology.master) {
        throw new Error('connectTopology requires discovered topology')
    }

    var master = await connectRedisNode('master', context.topology.master, context, createClient)
    var replicas = []

    for (var replica of context.topology.replicas || []) {
        replicas.push(await connectReplicaNode(replica, context, createClient))
    }

    return Object.freeze({
        ...context,
        master,
        replicas: Object.freeze(replicas)
    })
}

var indexConnectionsByKey = (connections = []) => {
    var byKey = new Map()

    for (var connection of connections) {
        byKey.set(connection.key, connection)
    }

    return byKey
}

var sameKeys = (left = [], right = []) => {
    if (left.length !== right.length) return false

    var leftKeys = left.map(item => item.key).sort()
    var rightKeys = right.map(item => item.key).sort()

    for (var i = 0; i < leftKeys.length; i += 1) {
        if (leftKeys[i] !== rightKeys[i]) return false
    }

    return true
}

var isSameTopology = (context, nextTopology) => {
    if (!context.master || !nextTopology || !nextTopology.master) return false
    if (context.master.key !== nextTopology.master.key) return false

    return sameKeys(context.replicas || [], nextTopology.replicas || [])
}

var closeRemovedConnections = async (connections, keepKeys) => {
    for (var connection of connections || []) {
        if (!keepKeys.has(connection.key)) await closeClient(connection.client)
    }
}

var applyTopology = async (context, nextTopology, createClient = createRedisClient) => {
    if (!context || context.mode !== 'sentinel') {
        throw new Error('applyTopology requires a sentinel redis context')
    }

    if (!nextTopology || !nextTopology.master) {
        throw new Error('applyTopology requires discovered topology')
    }

    if (isSameTopology(context, nextTopology)) return context

    var replicaNodes = nextTopology.replicas || []
    var replicaKeys = new Set(replicaNodes.map(replica => replica.key))
    var currentReplicas = context.replicas || []
    var currentReplicasByKey = indexConnectionsByKey(currentReplicas)
    var masterChanged = !context.master || context.master.key !== nextTopology.master.key
    var oldMaster = context.master
    var master = masterChanged
        ? await connectRedisNode('master', nextTopology.master, context, createClient)
        : context.master
    var replicas = []

    for (var replica of replicaNodes) {
        if (currentReplicasByKey.has(replica.key)) {
            replicas.push(currentReplicasByKey.get(replica.key))
        } else if (masterChanged && oldMaster && oldMaster.key === replica.key) {
            replicas.push(createConnectionRecord('replica', oldMaster.key, oldMaster.client, replica, oldMaster.status))
        } else {
            replicas.push(await connectReplicaNode(replica, context, createClient))
        }
    }

    await closeRemovedConnections(currentReplicas, replicaKeys)

    if (masterChanged && oldMaster && !replicaKeys.has(oldMaster.key)) {
        await closeClient(oldMaster.client)
    }

    return Object.freeze({
        ...context,
        master,
        replicas: Object.freeze(replicas),
        topology: Object.freeze({
            master: nextTopology.master,
            replicas: Object.freeze(replicaNodes)
        })
    })
}

var isMasterChanged = (context, topology) => {
    return Boolean(context.master && topology && topology.master && context.master.key !== topology.master.key)
}

var isMasterUnhealthy = (context) => {
    return Boolean(context.master && (context.master.status === 'suspect' || context.master.status === 'down'))
}

var confirmMasterTopology = async (context) => {
    var discovered = await discoverTopology(context)
    return discovered.topology
}

var handleMasterFailover = async (context, options = {}) => {
    if (!context || context.mode !== 'sentinel') {
        throw new Error('handleMasterFailover requires a sentinel redis context')
    }

    var topology = options.topology || await confirmMasterTopology(context)
    if (!isMasterChanged(context, topology)) return context

    return applyTopology(context, topology, options.createClient || createRedisClient)
}

var handlePossibleMasterFailover = async (context, options = {}) => {
    if (!options.force && !isMasterUnhealthy(context)) return context
    return handleMasterFailover(context, options)
}

var markSentinelSuspect = (context, err) => {
    return Object.freeze({
        ...context,
        sentinelStatus: 'suspect',
        lastReconcileError: err
    })
}

var markSentinelUp = (context) => {
    return Object.freeze({
        ...context,
        sentinelStatus: 'up',
        lastReconcileError: undefined
    })
}

var reconcileTopology = async (context, createClient = createRedisClient) => {
    try {
        var discovered = await discoverTopology(context)
        var applied = await applyTopology(context, discovered.topology, createClient)

        if (applied === context) return context
        return markSentinelUp(applied)
    } catch (err) {
        return markSentinelSuspect(context, err)
    }
}

var createScheduler = () => ({
    setInterval: (fn, ms) => setInterval(fn, ms),
    clearInterval: timer => clearInterval(timer),
    setTimeout: (fn, ms) => setTimeout(fn, ms),
    clearTimeout: timer => clearTimeout(timer)
})

var getReconcileInterval = (context, options = {}) => {
    return options.intervalMs || context.options.topologyIntervalMs || 5000
}

var createTopologyReconciler = (context, options = {}) => {
    var current = context
    var running = false
    var scheduler = options.scheduler || createScheduler()
    var createClient = options.createClient || createRedisClient
    var intervalMs = getReconcileInterval(context, options)
    var reconcile = async () => {
        if (running) return current

        running = true
        try {
            current = await reconcileTopology(current, createClient)
            return current
        } finally {
            running = false
        }
    }
    var timer = scheduler.setInterval(() => {
        reconcile().catch(() => undefined)
    }, intervalMs)

    return Object.freeze({
        getContext: () => current,
        reconcile,
        stop: () => scheduler.clearInterval(timer),
        timer,
        intervalMs
    })
}

var isSentinelTopologyEvent = (channel) => SENTINEL_TOPOLOGY_EVENTS.has(channel)

var getSentinelTopologyChannels = () => Object.freeze(Array.from(SENTINEL_TOPOLOGY_EVENTS))

var connectSentinelSubscriber = async (context, createClient = createRedisClient) => {
    if (!context || context.mode !== 'sentinel') {
        throw new Error('connectSentinelSubscriber requires a sentinel redis context')
    }

    var lastError = undefined

    for (var sentinel of context.sentinels) {
        var client = createClient(createSentinelClientOptions(sentinel, context))

        try {
            await client.connect()

            return Object.freeze({
                ...context,
                sentinelSubscriber: client
            })
        } catch (err) {
            lastError = err
            await closeClient(client)
        }
    }

    var error = new Error('Cannot connect sentinel subscriber, no sentinel available')
    error.cause = lastError
    throw error
}

var subscribeSentinelChannel = async (client, onEvent, channel) => {
    return client.subscribe(channel, message => onEvent(channel, message))
}

var subscribeSentinelChannels = async (client, onEvent, channels = getSentinelTopologyChannels()) => {
    for (var channel of channels) {
        await subscribeSentinelChannel(client, onEvent, channel)
    }
}

var createDebouncedReconcile = (reconcile, debounceMs, scheduler = createScheduler()) => {
    var timer = undefined
    var schedule = () => {
        if (timer) scheduler.clearTimeout(timer)

        timer = scheduler.setTimeout(() => {
            timer = undefined
            reconcile().catch(() => undefined)
        }, debounceMs)
    }
    var cancel = () => {
        if (!timer) return
        scheduler.clearTimeout(timer)
        timer = undefined
    }

    return Object.freeze({ schedule, cancel })
}

var createSentinelEventSubscription = async (context, reconciler, options = {}) => {
    var createClient = options.createClient || createRedisClient
    var scheduler = options.scheduler || createScheduler()
    var debounceMs = options.debounceMs || 250
    var nextContext = context.sentinelSubscriber
        ? context
        : await connectSentinelSubscriber(context, createClient)
    var debounced = createDebouncedReconcile(reconciler.reconcile, debounceMs, scheduler)
    var onEvent = (channel, message) => {
        if (!isSentinelTopologyEvent(channel)) return
        debounced.schedule(message)
    }

    await subscribeSentinelChannels(nextContext.sentinelSubscriber, onEvent, options.channels)

    return Object.freeze({
        context: nextContext,
        stop: async () => {
            debounced.cancel()
            await closeClient(nextContext.sentinelSubscriber)
        }
    })
}

var getDirectClient = (context) => {
    if (!context.master || !context.master.client) {
        throw new Error('direct redis context is not connected')
    }

    return context.master.client
}

var getMasterClient = (context) => {
    if (!context.master || !context.master.client) {
        throw new Error('sentinel redis context master is not connected')
    }

    return context.master.client
}

var classifyCommand = (args) => {
    if (!Array.isArray(args) || args.length === 0) {
        throw new Error('command args must be a non-empty array')
    }
    var name = String(args[0]).toUpperCase()
    if (READ_COMMANDS.has(name)) return 'read'
    if (WRITE_COMMANDS.has(name)) return 'write'
    return 'write'
}

var isHealthyConnection = (connection) => {
    return Boolean(connection && connection.status === 'up' && connection.client)
}

var chooseReplica = (context) => {
    return (context.replicas || []).find(isHealthyConnection)
}

var getCommandClient = (args, context) => {
    if (context.mode === 'direct') return getDirectClient(context)

    var type = classifyCommand(args)
    if (type === 'read') {
        var replica = chooseReplica(context)
        if (replica) return replica.client
    }

    return getMasterClient(context)
}


var command = async (args, context) => {
    if (!Array.isArray(args) || args.length === 0) {
        throw new Error('command args must be a non-empty array')
    }

    var client = getCommandClient(args, context)
    return client.sendCommand(args)
}

var closeClient = async (client) => {
    if (!client) return
    if (typeof client.close === 'function') return client.close()
    if (typeof client.destroy === 'function') return client.destroy()
    if (typeof client.disconnect === 'function') return client.disconnect()
    if (typeof client.quit === 'function') return client.quit()
}

var closeRedisContext = async (context) => {
    if (!context) return

    if (context && typeof context.ready === 'object' && typeof context.ready.then === 'function') {
        await context.ready.catch(() => undefined)
    }

    var hasSentinelEvents = Boolean(context.sentinelEvents)

    if (context.topologyReconciler) context.topologyReconciler.stop()
    if (context.sentinelHealer) context.sentinelHealer.stop()
    if (context.sentinelEvents) await context.sentinelEvents.stop()

    if (context.master) await closeClient(context.master.client)

    for (var replica of context.replicas || []) {
        await closeClient(replica.client)
    }

    await closeClient(context.sentinel)
    if (!hasSentinelEvents) await closeClient(context.sentinelSubscriber)
}

var attachBackground = async (context, options = {}) => {
    if (options.background === false || context.mode !== 'sentinel') return context

    var createClient = options.createClient || createRedisClient
    var dataCreateClient = options.dataCreateClient || createClient
    var topologyReconciler = createTopologyReconciler(context, {
        ...options,
        createClient: dataCreateClient
    })
    var sentinelHealer = createSentinelHealer(context, {
        ...options,
        createClient,
        dataCreateClient
    })
    var sentinelEvents = await createSentinelEventSubscription(context, topologyReconciler, {
        ...options,
        createClient
    })

    return Object.freeze({
        ...context,
        sentinelSubscriber: sentinelEvents.context.sentinelSubscriber,
        topologyReconciler,
        sentinelHealer,
        sentinelEvents
    })
}

var discoverRedis = async (context, options = {}) => {
    var createClient = options.createClient || createRedisClient

    if (context.mode === 'direct') return context

    var active = context.sentinel ? context : await connectSentinel(context, createClient)
    return discoverTopology(active)
}

var createRedis = (uri, options = {}) => {
    var context = createInitialContext(uri, options)
    var ready = discoverRedis(context, options)

    return Object.freeze({
        ...context,
        ready
    })
}

var connectMasterRedis = async (context, options = {}) => {
    var active = await (context && context.ready ? context.ready : Promise.resolve(context))

    if (!active || !active.uri) {
        throw new Error('connectMasterRedis requires a redis context returned by createRedis')
    }

    var createClient = options.createClient || options.dataCreateClient || active.options.dataCreateClient || active.options.createClient || createRedisClient
    var nextContext = undefined

    if (active.mode === 'direct') {
        nextContext = await connectDirect(active, createClient)
        return nextContext
    }

    if (!active.topology || !active.topology.master) {
        active = await discoverRedis(active, { ...active.options, ...options })
    }

    var master = await connectRedisNode('master', active.topology.master, active, createClient)
    nextContext = Object.freeze({
        ...active,
        master,
        replicas: Object.freeze([])
    })

    return nextContext
}

var connectRedis = async (context, options = {}) => {
    var active = await (context && context.ready ? context.ready : Promise.resolve(context))

    if (!active || !active.uri) {
        throw new Error('connectRedis requires a redis context returned by createRedis')
    }

    var createClient = options.createClient || active.options.createClient || createRedisClient
    var dataCreateClient = options.dataCreateClient || active.options.dataCreateClient || createClient
    var nextContext = undefined

    if (active.mode === 'direct') {
        nextContext = active.master ? active : await connectDirect(active, createClient)
        return nextContext
    }

    if (!active.sentinel) active = await connectSentinel(active, createClient)
    if (!active.topology || !active.topology.master) active = await discoverTopology(active)

    nextContext = active.master ? active : await connectTopology(active, dataCreateClient)
    nextContext = await attachBackground(nextContext, { ...active.options, ...options })

    return nextContext
}

var cloneRedis = (context, options = {}) => {
    var active = context

    if (!active || !active.uri) {
        throw new Error('cloneRedis requires a redis context returned by createRedis, not a Promise or empty value')
    }

    var nextContext = createInitialContext(active.uri, {
        ...active.options,
        ...options
    })

    if (active.topology) {
        nextContext = Object.freeze({
            ...nextContext,
            topology: active.topology
        })
    }

    return Object.freeze({
        ...nextContext,
        ready: Promise.resolve(nextContext)
    })
}

var closeRedis = async (context) => closeRedisContext(context)

module.exports = {
    createRedis,
    connectRedis,
    connect: connectRedis,
    connectMasterRedis,
    connectMaster: connectMasterRedis,
    cloneRedis,
    clone: cloneRedis,
    closeRedis,
    parseRedisUrl,
    createInitialContext,
    parseSentinelNode,
    parsePort,
    createRedisClient,
    createConnectionRecord,
    getHealthOptions,
    createRetryState,
    getBackoffDelay,
    nextRetryState,
    markConnectionFailure,
    markConnectionSuccess,
    canRetryNow,
    createClientHealthRecord,
    getContextHealthRecord,
    setContextHealthRecord,
    markContextConnectionFailure,
    markContextConnectionSuccess,
    createDownConnectionRecord,
    createRedisNodeClientOptions,
    connectRedisNode,
    connectDirect,
    createSentinelClientOptions,
    connectSentinel,
    createSentinelKey,
    getSentinelHealthMap,
    getSentinelCandidateHealth,
    setSentinelCandidateHealth,
    markSentinelCandidateFailure,
    markSentinelCandidateSuccess,
    getNextSentinelIndexes,
    canTrySentinelCandidate,
    connectSentinelCandidate,
    healSentinelOnce,
    getSentinelHealInterval,
    createSentinelHealer,
    createNodeKey,
    normalizeMaster,
    normalizeReplicas,
    discoverMaster,
    discoverReplicas,
    discoverTopology,
    connectTopology,
    indexConnectionsByKey,
    sameKeys,
    isSameTopology,
    applyTopology,
    isMasterChanged,
    isMasterUnhealthy,
    confirmMasterTopology,
    handleMasterFailover,
    handlePossibleMasterFailover,
    markSentinelSuspect,
    markSentinelUp,
    reconcileTopology,
    createScheduler,
    getReconcileInterval,
    createTopologyReconciler,
    SENTINEL_TOPOLOGY_EVENTS,
    isSentinelTopologyEvent,
    getSentinelTopologyChannels,
    connectSentinelSubscriber,
    subscribeSentinelChannel,
    subscribeSentinelChannels,
    createDebouncedReconcile,
    createSentinelEventSubscription,
    discoverRedis,
    READ_COMMANDS,
    WRITE_COMMANDS,
    classifyCommand,
    isHealthyConnection,
    chooseReplica,
    getCommandClient,
    attachBackground,
    command,
    closeRedisContext
}
