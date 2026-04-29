'use strict'

var redis = require('redis')

var SENTINEL_PROTOCOL = 'redis+sentinel://'

var assertString = (value, name) => {
    if (typeof value !== 'string' || value.trim() === '') {
        throw new Error(`${name} must be a non-empty string`)
    }
}

var parsePort = (value, defaultPort, label) => {
    if (value == null || value === '') return defaultPort

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

var createConnectionRecord = (role, key, client) => {
    return Object.freeze({
        key,
        role,
        client,
        status: 'up',
        failures: 0,
        retry: Object.freeze({
            attempt: 0,
            delay: 0,
            next: 0
        })
    })
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
    connectDirect,
    createSentinelClientOptions,
    connectSentinel,
    command,
    closeRedisContext
}
