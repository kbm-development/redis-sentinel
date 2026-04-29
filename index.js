'use strict'

var redis = require('redis')

var SENTINEL_PROTOCOL = 'redis+sentinel://'

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

    for (var sentinel of context.sentinels) {
        var client = createClient(createSentinelClientOptions(sentinel, context))

        try {
            await client.connect()

            return Object.freeze({
                ...context,
                sentinel: client
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

var getDirectClient = (context) => {
    if (!context || context.mode !== 'direct') {
        throw new Error('direct command requires a direct redis context')
    }

    if (!context.master || !context.master.client) {
        throw new Error('direct redis context is not connected')
    }

    return context.master.client
}

var command = async (args, context) => {
    if (!Array.isArray(args) || args.length === 0) {
        throw new Error('command args must be a non-empty array')
    }

    if (context.mode === 'direct') {
        var client = getDirectClient(context)
        return client.sendCommand(args)
    }

    throw new Error('sentinel command routing is not implemented yet')
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

    if (context.master) await closeClient(context.master.client)

    for (var replica of context.replicas || []) {
        await closeClient(replica.client)
    }

    await closeClient(context.sentinel)
    await closeClient(context.sentinelSubscriber)
}

module.exports = {
    parseRedisUrl,
    createInitialContext,
    parseSentinelNode,
    parsePort,
    createRedisClient,
    createConnectionRecord,
    createDownConnectionRecord,
    createRedisNodeClientOptions,
    connectRedisNode,
    connectDirect,
    createSentinelClientOptions,
    connectSentinel,
    createNodeKey,
    normalizeMaster,
    normalizeReplicas,
    discoverMaster,
    discoverReplicas,
    discoverTopology,
    connectTopology,
    command,
    closeRedisContext
}
