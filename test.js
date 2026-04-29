'use strict'

var assert = require('node:assert/strict')
var {
    parseRedisUrl,
    createInitialContext,
    parseSentinelNode,
    parsePort,
    connectDirect,
    createSentinelClientOptions,
    connectSentinel,
    normalizeMaster,
    normalizeReplicas,
    discoverMaster,
    discoverReplicas,
    discoverTopology,
    getBackoffDelay,
    nextRetryState,
    markConnectionFailure,
    markConnectionSuccess,
    canRetryNow,
    createClientHealthRecord,
    markContextConnectionFailure,
    markContextConnectionSuccess,
    createRedisNodeClientOptions,
    connectRedisNode,
    connectTopology,
    applyTopology,
    reconcileTopology,
    getReconcileInterval,
    createTopologyReconciler,
    isSentinelTopologyEvent,
    getSentinelTopologyChannels,
    connectSentinelSubscriber,
    subscribeSentinelChannels,
    createDebouncedReconcile,
    createSentinelEventSubscription,
    getSentinelCandidateHealth,
    getNextSentinelIndexes,
    canTrySentinelCandidate,
    healSentinelOnce,
    getSentinelHealInterval,
    createSentinelHealer,
    classifyCommand,
    chooseReplica,
    command,
    closeRedisContext
} = require('./index')

var tests = []

var test = (name, fn) => {
    tests.push({ name, fn })
}

test('parsePort returns default port when empty', () => {
    assert.equal(parsePort(undefined, 26379, 'sentinel'), 26379)
    assert.equal(parsePort('', 6379, 'redis'), 6379)
})

test('parsePort validates TCP port range', () => {
    assert.equal(parsePort('26379', 6379, 'sentinel'), 26379)
    assert.throws(() => parsePort('0', 6379, 'sentinel'), /valid TCP port/)
    assert.throws(() => parsePort('65536', 6379, 'sentinel'), /valid TCP port/)
    assert.throws(() => parsePort('abc', 6379, 'sentinel'), /valid TCP port/)
})

test('parseSentinelNode parses host and explicit port', () => {
    assert.deepEqual(parseSentinelNode('sentinel-a.local:26380'), {
        host: 'sentinel-a.local',
        port: 26380
    })
})

test('parseSentinelNode defaults missing port', () => {
    assert.deepEqual(parseSentinelNode('sentinel-a.local'), {
        host: 'sentinel-a.local',
        port: 26379
    })
})

test('parseRedisUrl returns direct mode for non sentinel URL', () => {
    var parsed = parseRedisUrl('redis://redis.local:6379')

    assert.equal(parsed.mode, 'direct')
    assert.equal(parsed.uri, 'redis://redis.local:6379')
    assert.deepEqual(parsed.sentinels, [])
    assert.equal(parsed.masterName, undefined)
})

test('parseRedisUrl parses sentinel URL without auth', () => {
    var parsed = parseRedisUrl('redis+sentinel://s1.local:26379,s2.local?sentinelMasterId=mymaster')

    assert.equal(parsed.mode, 'sentinel')
    assert.equal(parsed.masterName, 'mymaster')
    assert.equal(parsed.username, undefined)
    assert.equal(parsed.password, undefined)
    assert.deepEqual(parsed.sentinels, [
        { host: 's1.local', port: 26379 },
        { host: 's2.local', port: 26379 }
    ])
})

test('parseRedisUrl parses sentinel URL with auth', () => {
    var parsed = parseRedisUrl('redis+sentinel://user:pass@s1.local:26379?sentinelMasterId=mymaster')

    assert.equal(parsed.mode, 'sentinel')
    assert.equal(parsed.username, 'user')
    assert.equal(parsed.password, 'pass')
    assert.deepEqual(parsed.sentinels, [{ host: 's1.local', port: 26379 }])
})

test('parseRedisUrl validates sentinel hosts', () => {
    assert.throws(
        () => parseRedisUrl('redis+sentinel://?sentinelMasterId=mymaster'),
        /at least one sentinel host/
    )
})

test('parseRedisUrl validates sentinel master name', () => {
    assert.throws(
        () => parseRedisUrl('redis+sentinel://s1.local:26379'),
        /sentinelMasterId/
    )
})

test('createInitialContext creates empty direct context', () => {
    var context = createInitialContext('redis://redis.local:6379', { topologyIntervalMs: 5000 })

    assert.equal(context.mode, 'direct')
    assert.equal(context.master, undefined)
    assert.deepEqual(context.replicas, [])
    assert.equal(context.topology, undefined)
    assert.deepEqual(context.options, { topologyIntervalMs: 5000 })
})

test('createInitialContext creates empty sentinel context', () => {
    var context = createInitialContext('redis+sentinel://s1.local:26379?sentinelMasterId=mymaster')
    assert.equal(context.mode, 'sentinel')
    assert.equal(context.masterName, 'mymaster')
    assert.equal(context.sentinel, undefined)
    assert.equal(context.sentinelSubscriber, undefined)
    assert.deepEqual(context.replicas, [])
    assert.deepEqual(context.timers, {})
})


test('createInitialContext from env REDIS_URL only sentinel', ()=>{
    if (!process.env.REDIS_URL) return
    var context = createInitialContext(process.env.REDIS_URL);
	if(context.mode === 'sentinel'){
		assert.equal(context.mode, 'sentinel')
		assert.equal(context.masterName, 'mymaster')
		assert.equal(context.sentinel, undefined)
		assert.equal(context.sentinelSubscriber, undefined)
		assert.deepEqual(context.replicas, [])
		assert.deepEqual(context.timers, {})
	}
	assert.equal(1,1) // just pass
})


test('connectDirect creates and connects direct redis client', async () => {
    var createdOptions = undefined
    var calls = []
    var fakeClient = {
        connect: async () => calls.push('connect'),
        sendCommand: async () => 'ok',
        quit: async () => calls.push('quit')
    }
    var createClient = (options) => {
        createdOptions = options
        return fakeClient
    }
    var context = createInitialContext('redis://redis.local:6379')
    var connected = await connectDirect(context, createClient)

    assert.deepEqual(createdOptions, { url: 'redis://redis.local:6379' })
    assert.deepEqual(calls, ['connect'])
    assert.equal(connected.mode, 'direct')
    assert.equal(connected.master.role, 'master')
    assert.equal(connected.master.key, 'redis://redis.local:6379')
    assert.equal(connected.master.client, fakeClient)
    assert.equal(connected.master.status, 'up')
})

test('connectDirect rejects sentinel context', async () => {
    var context = createInitialContext('redis+sentinel://s1.local:26379?sentinelMasterId=mymaster')

    await assert.rejects(
        () => connectDirect(context, () => ({})),
        /direct redis context/
    )
})

test('command sends direct mode command to connected client', async () => {
    var sentArgs = undefined
    var fakeClient = {
        connect: async () => undefined,
        sendCommand: async (args) => {
            sentArgs = args
            return 'value'
        }
    }
    var context = createInitialContext('redis://redis.local:6379')
    var connected = await connectDirect(context, () => fakeClient)
    var result = await command(['GET', 'key'], connected)

    assert.deepEqual(sentArgs, ['GET', 'key'])
    assert.equal(result, 'value')
})

test('command surfaces direct client errors without retry', async () => {
    var attempts = 0
    var fakeClient = {
        connect: async () => undefined,
        sendCommand: async () => {
            attempts += 1
            throw new Error('boom')
        }
    }
    var context = createInitialContext('redis://redis.local:6379')
    var connected = await connectDirect(context, () => fakeClient)

    await assert.rejects(() => command(['SET', 'key', 'value'], connected), /boom/)
    assert.equal(attempts, 1)
})

test('command validates direct context is connected', async () => {
    var context = createInitialContext('redis://redis.local:6379')

    await assert.rejects(() => command(['GET', 'key'], context), /not connected/)
})

test('closeRedisContext closes direct master client', async () => {
    var calls = []
    var fakeClient = {
        connect: async () => undefined,
        sendCommand: async () => 'ok',
        quit: async () => calls.push('quit')
    }
    var context = createInitialContext('redis://redis.local:6379')
    var connected = await connectDirect(context, () => fakeClient)

    await closeRedisContext(connected)

    assert.deepEqual(calls, ['quit'])
});

test('closeRedisContext prefers socket close over quit command', async () => {
    var calls = []
    var fakeClient = {
        connect: async () => undefined,
        sendCommand: async () => 'ok',
        close: async () => calls.push('close'),
        quit: async () => calls.push('quit')
    }
    var context = createInitialContext('redis://redis.local:6379')
    var connected = await connectDirect(context, () => fakeClient)

    await closeRedisContext(connected)

    assert.deepEqual(calls, ['close'])
});

test('markConnectionFailure marks first failure suspect', () => {
    var connection = {
        key: 'replica-a:6379',
        role: 'replica',
        client: {},
        status: 'up',
        failures: 0,
        retry: { attempt: 0, delay: 0, next: 0 }
    }
    var result = markConnectionFailure(connection, {
        failureThreshold: 2,
        baseDelay: 100,
        maxDelay: 1000,
        now: 1000
    })

    assert.equal(result.status, 'suspect')
    assert.equal(result.failures, 1)
    assert.deepEqual(result.retry, { attempt: 1, delay: 100, next: 1100 })
})

test('markConnectionFailure marks repeated failure down', () => {
    var connection = {
        key: 'replica-a:6379',
        role: 'replica',
        client: {},
        status: 'suspect',
        failures: 1,
        retry: { attempt: 1, delay: 100, next: 1100 }
    }
    var result = markConnectionFailure(connection, {
        failureThreshold: 2,
        baseDelay: 100,
        maxDelay: 1000,
        now: 2000
    })

    assert.equal(result.status, 'down')
    assert.equal(result.failures, 2)
    assert.deepEqual(result.retry, { attempt: 2, delay: 200, next: 2200 })
})

test('markConnectionSuccess resets health and retry state', () => {
    var connection = {
        key: 'replica-a:6379',
        role: 'replica',
        client: {},
        status: 'down',
        failures: 3,
        retry: { attempt: 3, delay: 400, next: 1400 }
    }
    var result = markConnectionSuccess(connection)

    assert.equal(result.status, 'up')
    assert.equal(result.failures, 0)
    assert.deepEqual(result.retry, { attempt: 0, delay: 0, next: 0 })
})

test('nextRetryState uses bounded exponential backoff', () => {
    assert.deepEqual(nextRetryState({ attempt: 0 }, { baseDelay: 100, maxDelay: 250, now: 1000 }), {
        attempt: 1,
        delay: 100,
        next: 1100
    })
    assert.deepEqual(nextRetryState({ attempt: 2 }, { baseDelay: 100, maxDelay: 250, now: 1000 }), {
        attempt: 3,
        delay: 250,
        next: 1250
    })
    assert.equal(getBackoffDelay(10, { baseDelay: 100, maxDelay: 500 }), 500)
})

test('canRetryNow checks per-connection retry time', () => {
    assert.equal(canRetryNow({ retry: { next: 1000 } }, 999), false)
    assert.equal(canRetryNow({ retry: { next: 1000 } }, 1000), true)
    assert.equal(canRetryNow({ retry: { next: 0 } }, 1), true)
})

test('context health updates are independent per connection', () => {
    var context = Object.freeze({
        master: {
            key: 'master:6379',
            role: 'master',
            client: {},
            status: 'up',
            failures: 0,
            retry: { attempt: 0, delay: 0, next: 0 }
        },
        replicas: Object.freeze([
            {
                key: 'replica-a:6379',
                role: 'replica',
                client: {},
                status: 'up',
                failures: 0,
                retry: { attempt: 0, delay: 0, next: 0 }
            },
            {
                key: 'replica-b:6379',
                role: 'replica',
                client: {},
                status: 'up',
                failures: 0,
                retry: { attempt: 0, delay: 0, next: 0 }
            }
        ])
    })
    var result = markContextConnectionFailure(context, { replicaKey: 'replica-a:6379' }, {
        failureThreshold: 2,
        baseDelay: 100,
        maxDelay: 1000,
        now: 1000
    })

    assert.equal(result.master, context.master)
    assert.equal(result.replicas[0].status, 'suspect')
    assert.equal(result.replicas[1], context.replicas[1])
    assert.notEqual(result.replicas[0].retry, context.replicas[0].retry)
})

test('context health tracks sentinel and subscriber separately', () => {
    var sentinelClient = {}
    var subscriberClient = {}
    var context = Object.freeze({
        sentinel: sentinelClient,
        sentinelSubscriber: subscriberClient
    })
    var failedSentinel = markContextConnectionFailure(context, 'sentinel', {
        failureThreshold: 2,
        baseDelay: 100,
        maxDelay: 1000,
        now: 1000
    })
    var failedSubscriber = markContextConnectionFailure(failedSentinel, 'sentinelSubscriber', {
        failureThreshold: 2,
        baseDelay: 200,
        maxDelay: 1000,
        now: 1000
    })
    var healedSentinel = markContextConnectionSuccess(failedSubscriber, 'sentinel')

    assert.equal(failedSubscriber.sentinelHealth.status, 'suspect')
    assert.equal(failedSubscriber.sentinelSubscriberHealth.status, 'suspect')
    assert.deepEqual(failedSubscriber.sentinelHealth.retry, { attempt: 1, delay: 100, next: 1100 })
    assert.deepEqual(failedSubscriber.sentinelSubscriberHealth.retry, { attempt: 1, delay: 200, next: 1200 })
    assert.equal(healedSentinel.sentinelHealth.status, 'up')
    assert.equal(healedSentinel.sentinelSubscriberHealth.status, 'suspect')
})

test('createClientHealthRecord stores client health state', () => {
    var client = {}
    var health = createClientHealthRecord('sentinel', client)

    assert.equal(health.key, 'sentinel')
    assert.equal(health.role, 'sentinel')
    assert.equal(health.client, client)
    assert.equal(health.status, 'up')
})

test('createSentinelClientOptions creates node redis sentinel socket options', () => {
    var context = createInitialContext('redis+sentinel://user:pass@s1.local:26379?sentinelMasterId=mymaster')
    var options = createSentinelClientOptions(context.sentinels[0], context)

    assert.deepEqual(options, {
        socket: {
            host: 's1.local',
            port: 26379
        },
        username: 'user',
        password: 'pass'
    })
})

test('connectSentinel connects first available sentinel', async () => {
    var createdOptions = []
    var calls = []
    var fakeClient = {
        connect: async () => calls.push('connect'),
        quit: async () => calls.push('quit')
    }
    var createClient = (options) => {
        createdOptions.push(options)
        return fakeClient
    }
    var context = createInitialContext('redis+sentinel://s1.local:26379,s2.local:26380?sentinelMasterId=mymaster')
    var connected = await connectSentinel(context, createClient)

    assert.equal(connected.sentinel, fakeClient)
    assert.equal(connected.sentinels.length, 2)
    assert.deepEqual(calls, ['connect'])
    assert.deepEqual(createdOptions, [{
        socket: {
            host: 's1.local',
            port: 26379
        },
        username: undefined,
        password: undefined
    }])
})

test('connectSentinel tries next sentinel when one fails', async () => {
    var calls = []
    var firstClient = {
        connect: async () => {
            calls.push('connect first')
            throw new Error('first failed')
        },
        quit: async () => calls.push('quit first')
    }
    var secondClient = {
        connect: async () => calls.push('connect second'),
        quit: async () => calls.push('quit second')
    }
    var clients = [firstClient, secondClient]
    var createClient = () => clients.shift()
    var context = createInitialContext('redis+sentinel://s1.local:26379,s2.local:26380?sentinelMasterId=mymaster')
    var connected = await connectSentinel(context, createClient)

    assert.equal(connected.sentinel, secondClient)
    assert.deepEqual(calls, ['connect first', 'quit first', 'connect second'])
})

test('connectSentinel rejects when all sentinels fail', async () => {
    var calls = []
    var createClient = () => ({
        connect: async () => {
            calls.push('connect')
            throw new Error('failed')
        },
        quit: async () => calls.push('quit')
    })
    var context = createInitialContext('redis+sentinel://s1.local:26379,s2.local:26380?sentinelMasterId=mymaster')

    await assert.rejects(
        () => connectSentinel(context, createClient),
        /no sentinel available/
    )
    assert.deepEqual(calls, ['connect', 'quit', 'connect', 'quit'])
})

test('connectSentinel rejects direct context', async () => {
    var context = createInitialContext('redis://redis.local:6379')

    await assert.rejects(
        () => connectSentinel(context, () => ({})),
        /sentinel redis context/
    )
})

test('getNextSentinelIndexes starts after current sentinel', () => {
    var context = Object.freeze({
        sentinelIndex: 1,
        sentinels: Object.freeze([
            { host: 's1.local', port: 26379 },
            { host: 's2.local', port: 26379 },
            { host: 's3.local', port: 26379 }
        ])
    })

    assert.deepEqual(getNextSentinelIndexes(context), [2, 0, 1])
})

test('canTrySentinelCandidate respects per candidate retry state', () => {
    var sentinel = { host: 's1.local', port: 26379 }
    var context = Object.freeze({
        sentinelCandidateHealth: Object.freeze({
            's1.local:26379': Object.freeze({
                key: 's1.local:26379',
                retry: Object.freeze({ next: 2000 })
            })
        })
    })

    assert.equal(canTrySentinelCandidate(context, sentinel, 1000), false)
    assert.equal(canTrySentinelCandidate(context, sentinel, 2000), true)
})

test('healSentinelOnce reconnects next sentinel and reconciles topology', async () => {
    var calls = []
    var oldSentinelClient = { close: async () => calls.push('close old sentinel') }
    var newSentinelClient = {
        connect: async () => calls.push('connect s2'),
        close: async () => calls.push('close s2'),
        sendCommand: async (args) => {
            calls.push(args[1])
            if (args[1] === 'get-master-addr-by-name') return ['master', '6379']
            return [['ip', 'replica-a', 'port', '6379', 'flags', 'slave']]
        }
    }
    var context = Object.freeze({
        mode: 'sentinel',
        masterName: 'mymaster',
        options: Object.freeze({}),
        sentinelIndex: 0,
        sentinels: Object.freeze([
            { host: 's1.local', port: 26379 },
            { host: 's2.local', port: 26379 }
        ]),
        sentinel: oldSentinelClient,
        master: { key: 'master:6379', client: {}, status: 'up' },
        replicas: Object.freeze([{ key: 'replica-a:6379', client: {}, status: 'up' }])
    })
    var result = await healSentinelOnce(context, {
        now: 1000,
        createClient: () => newSentinelClient,
        dataCreateClient: () => {
            throw new Error('should not connect data plane')
        }
    })

    assert.deepEqual(calls, ['connect s2', 'close old sentinel', 'get-master-addr-by-name', 'replicas'])
    assert.equal(result.sentinel, newSentinelClient)
    assert.equal(result.sentinelIndex, 1)
    assert.equal(result.master, context.master)
    assert.equal(result.replicas, context.replicas)
    assert.equal(getSentinelCandidateHealth(result, { host: 's2.local', port: 26379 }).status, 'up')
})

test('healSentinelOnce skips candidates still in backoff', async () => {
    var calls = []
    var s1Client = {
        connect: async () => calls.push('connect s1'),
        sendCommand: async (args) => {
            if (args[1] === 'get-master-addr-by-name') return ['master', '6379']
            return []
        }
    }
    var context = Object.freeze({
        mode: 'sentinel',
        masterName: 'mymaster',
        options: Object.freeze({}),
        sentinelIndex: 0,
        sentinels: Object.freeze([
            { host: 's1.local', port: 26379 },
            { host: 's2.local', port: 26379 }
        ]),
        sentinelCandidateHealth: Object.freeze({
            's2.local:26379': Object.freeze({
                key: 's2.local:26379',
                retry: Object.freeze({ next: 5000 })
            })
        }),
        master: { key: 'master:6379', client: {}, status: 'up' },
        replicas: Object.freeze([])
    })
    var result = await healSentinelOnce(context, {
        now: 1000,
        createClient: () => s1Client,
        dataCreateClient: () => {
            throw new Error('should not connect data plane')
        }
    })

    assert.deepEqual(calls, ['connect s1'])
    assert.equal(result.sentinelIndex, 0)
})

test('healSentinelOnce keeps data plane alive when all sentinels fail', async () => {
    var calls = []
    var masterClient = { close: async () => calls.push('close master') }
    var replicaClient = { close: async () => calls.push('close replica') }
    var createClient = () => ({
        connect: async () => {
            calls.push('connect sentinel')
            throw new Error('sentinel failed')
        },
        close: async () => calls.push('close failed sentinel')
    })
    var context = Object.freeze({
        mode: 'sentinel',
        options: Object.freeze({}),
        sentinelIndex: 0,
        sentinels: Object.freeze([
            { host: 's1.local', port: 26379 },
            { host: 's2.local', port: 26379 }
        ]),
        master: { key: 'master:6379', client: masterClient, status: 'up' },
        replicas: Object.freeze([{ key: 'replica-a:6379', client: replicaClient, status: 'up' }])
    })
    var result = await healSentinelOnce(context, {
        failureThreshold: 1,
        now: 1000,
        createClient
    })

    assert.deepEqual(calls, ['connect sentinel', 'close failed sentinel', 'connect sentinel', 'close failed sentinel'])
    assert.equal(result.master, context.master)
    assert.equal(result.replicas, context.replicas)
    assert.equal(result.sentinelStatus, 'suspect')
    assert.equal(getSentinelCandidateHealth(result, { host: 's1.local', port: 26379 }).status, 'down')
    assert.equal(getSentinelCandidateHealth(result, { host: 's2.local', port: 26379 }).status, 'down')
})

test('getSentinelHealInterval uses option, context option, then default', () => {
    assert.equal(getSentinelHealInterval({ options: { sentinelHealIntervalMs: 3000 } }, { intervalMs: 1000 }), 1000)
    assert.equal(getSentinelHealInterval({ options: { sentinelHealIntervalMs: 3000 } }, {}), 3000)
    assert.equal(getSentinelHealInterval({ options: {} }, {}), 1000)
})

test('createSentinelHealer starts timer and updates current context', async () => {
    var scheduled = undefined
    var cleared = undefined
    var calls = []
    var scheduler = {
        setInterval: (fn, ms) => {
            scheduled = { fn, ms }
            return 'sentinel-heal-timer'
        },
        clearInterval: timer => {
            cleared = timer
        }
    }
    var sentinelClient = {
        connect: async () => calls.push('connect sentinel'),
        sendCommand: async (args) => {
            if (args[1] === 'get-master-addr-by-name') return ['master', '6379']
            return []
        }
    }
    var context = Object.freeze({
        mode: 'sentinel',
        masterName: 'mymaster',
        options: Object.freeze({ sentinelHealIntervalMs: 2222 }),
        sentinelIndex: 0,
        sentinels: Object.freeze([{ host: 's1.local', port: 26379 }]),
        master: { key: 'master:6379', client: {}, status: 'up' },
        replicas: Object.freeze([])
    })
    var healer = createSentinelHealer(context, {
        scheduler,
        createClient: () => sentinelClient,
        dataCreateClient: () => {
            throw new Error('should not connect data plane')
        }
    })

    assert.equal(scheduled.ms, 2222)
    assert.equal(healer.getContext(), context)

    await healer.heal()

    assert.deepEqual(calls, ['connect sentinel'])
    assert.equal(healer.getContext().sentinel, sentinelClient)

    healer.stop()

    assert.equal(cleared, 'sentinel-heal-timer')
})

test('normalizeMaster creates stable master topology record', () => {
    assert.deepEqual(normalizeMaster(['redis-master.local', '6379']), {
        host: 'redis-master.local',
        port: 6379,
        key: 'redis-master.local:6379'
    })
})

test('normalizeReplicas creates stable replica records and skips down replicas', () => {
    var replicas = normalizeReplicas([
        ['name', 'replica-a', 'ip', '10.0.0.2', 'port', '6379', 'flags', 'slave'],
        ['name', 'replica-b', 'ip', '10.0.0.3', 'port', '6379', 'flags', 's_down,slave'],
        ['name', 'replica-c', 'ip', '10.0.0.4', 'port', '6380', 'flags', 'slave']
    ])

    assert.deepEqual(replicas, [
        { host: '10.0.0.2', port: 6379, key: '10.0.0.2:6379' },
        { host: '10.0.0.4', port: 6380, key: '10.0.0.4:6380' }
    ])
})

test('discoverMaster sends sentinel master command', async () => {
    var sentArgs = undefined
    var context = createInitialContext('redis+sentinel://s1.local:26379?sentinelMasterId=mymaster')
    context = Object.freeze({
        ...context,
        sentinel: {
            sendCommand: async (args) => {
                sentArgs = args
                return ['redis-master.local', '6379']
            }
        }
    })
    var master = await discoverMaster(context)

    assert.deepEqual(sentArgs, ['SENTINEL', 'get-master-addr-by-name', 'mymaster'])
    assert.deepEqual(master, {
        host: 'redis-master.local',
        port: 6379,
        key: 'redis-master.local:6379'
    })
})

test('discoverReplicas sends sentinel replicas command', async () => {
    var sentArgs = undefined
    var context = createInitialContext('redis+sentinel://s1.local:26379?sentinelMasterId=mymaster')
    context = Object.freeze({
        ...context,
        sentinel: {
            sendCommand: async (args) => {
                sentArgs = args
                return [[
                    'name', 'replica-a',
                    'ip', '10.0.0.2',
                    'port', '6379',
                    'flags', 'slave'
                ]]
            }
        }
    })
    var replicas = await discoverReplicas(context)

    assert.deepEqual(sentArgs, ['SENTINEL', 'replicas', 'mymaster'])
    assert.deepEqual(replicas, [
        { host: '10.0.0.2', port: 6379, key: '10.0.0.2:6379' }
    ])
})

test('discoverTopology stores master and replicas without data connections', async () => {
    var sentCommands = []
    var context = createInitialContext('redis+sentinel://s1.local:26379?sentinelMasterId=mymaster')
    context = Object.freeze({
        ...context,
        sentinel: {
            sendCommand: async (args) => {
                sentCommands.push(args)
                if (args[1] === 'get-master-addr-by-name') return ['redis-master.local', '6379']
                return [[
                    'name', 'replica-a',
                    'ip', '10.0.0.2',
                    'port', '6379',
                    'flags', 'slave'
                ]]
            }
        }
    })
    var discovered = await discoverTopology(context)

    assert.deepEqual(sentCommands, [
        ['SENTINEL', 'get-master-addr-by-name', 'mymaster'],
        ['SENTINEL', 'replicas', 'mymaster']
    ])
    assert.equal(discovered.master, undefined)
    assert.deepEqual(discovered.replicas, [])
    assert.deepEqual(discovered.topology, {
        master: { host: 'redis-master.local', port: 6379, key: 'redis-master.local:6379' },
        replicas: [{ host: '10.0.0.2', port: 6379, key: '10.0.0.2:6379' }]
    })
})

test('discoverTopology validates sentinel context is connected', async () => {
    var context = createInitialContext('redis+sentinel://s1.local:26379?sentinelMasterId=mymaster')

    await assert.rejects(() => discoverTopology(context), /not connected/)
})

test('createRedisNodeClientOptions creates node redis data socket options', () => {
    var context = createInitialContext('redis+sentinel://user:pass@s1.local:26379?sentinelMasterId=mymaster')
    var options = createRedisNodeClientOptions({ host: 'redis-master.local', port: 6379 }, context)

    assert.deepEqual(options, {
        socket: {
            host: 'redis-master.local',
            port: 6379
        },
        username: 'user',
        password: 'pass'
    })
})

test('connectRedisNode connects one data node and returns connection record', async () => {
    var createdOptions = undefined
    var calls = []
    var fakeClient = {
        connect: async () => calls.push('connect')
    }
    var createClient = (options) => {
        createdOptions = options
        return fakeClient
    }
    var context = createInitialContext('redis+sentinel://s1.local:26379?sentinelMasterId=mymaster')
    var node = { host: 'redis-master.local', port: 6379, key: 'redis-master.local:6379' }
    var connection = await connectRedisNode('master', node, context, createClient)

    assert.deepEqual(createdOptions, {
        socket: { host: 'redis-master.local', port: 6379 },
        username: undefined,
        password: undefined
    })
    assert.deepEqual(calls, ['connect'])
    assert.equal(connection.role, 'master')
    assert.equal(connection.key, 'redis-master.local:6379')
    assert.equal(connection.host, 'redis-master.local')
    assert.equal(connection.port, 6379)
    assert.equal(connection.client, fakeClient)
    assert.equal(connection.status, 'up')
})

test('connectTopology connects master and reachable replicas', async () => {
    var calls = []
    var createdOptions = []
    var createFakeClient = (name) => ({
        connect: async () => calls.push(`connect ${name}`),
        close: async () => calls.push(`close ${name}`)
    })
    var clients = [
        createFakeClient('master'),
        createFakeClient('replica-a'),
        createFakeClient('replica-b')
    ]
    var createClient = (options) => {
        createdOptions.push(options)
        return clients.shift()
    }
    var context = createInitialContext('redis+sentinel://s1.local:26379?sentinelMasterId=mymaster')
    context = Object.freeze({
        ...context,
        topology: Object.freeze({
            master: { host: 'redis-master.local', port: 6379, key: 'redis-master.local:6379' },
            replicas: Object.freeze([
                { host: 'redis-replica-a.local', port: 6379, key: 'redis-replica-a.local:6379' },
                { host: 'redis-replica-b.local', port: 6380, key: 'redis-replica-b.local:6380' }
            ])
        })
    })
    var connected = await connectTopology(context, createClient)

    assert.deepEqual(calls, ['connect master', 'connect replica-a', 'connect replica-b'])
    assert.equal(createdOptions.length, 3)
    assert.equal(connected.master.role, 'master')
    assert.equal(connected.master.status, 'up')
    assert.equal(connected.master.key, 'redis-master.local:6379')
    assert.equal(connected.replicas.length, 2)
    assert.deepEqual(connected.replicas.map(replica => replica.status), ['up', 'up'])
})

test('connectTopology marks failed replicas down without failing startup', async () => {
    var calls = []
    var masterClient = {
        connect: async () => calls.push('connect master')
    }
    var replicaClient = {
        connect: async () => {
            calls.push('connect replica')
            throw new Error('replica failed')
        },
        close: async () => calls.push('close replica')
    }
    var clients = [masterClient, replicaClient]
    var createClient = () => clients.shift()
    var context = createInitialContext('redis+sentinel://s1.local:26379?sentinelMasterId=mymaster')
    context = Object.freeze({
        ...context,
        topology: Object.freeze({
            master: { host: 'redis-master.local', port: 6379, key: 'redis-master.local:6379' },
            replicas: Object.freeze([
                { host: 'redis-replica.local', port: 6379, key: 'redis-replica.local:6379' }
            ])
        })
    })
    var connected = await connectTopology(context, createClient)

    assert.deepEqual(calls, ['connect master', 'connect replica', 'close replica'])
    assert.equal(connected.master.status, 'up')
    assert.equal(connected.replicas.length, 1)
    assert.equal(connected.replicas[0].status, 'down')
    assert.equal(connected.replicas[0].client, undefined)
    assert.equal(connected.replicas[0].failures, 1)
})

test('connectTopology rejects when master connection fails', async () => {
    var calls = []
    var createClient = () => ({
        connect: async () => {
            calls.push('connect master')
            throw new Error('master failed')
        },
        close: async () => calls.push('close master')
    })
    var context = createInitialContext('redis+sentinel://s1.local:26379?sentinelMasterId=mymaster')
    context = Object.freeze({
        ...context,
        topology: Object.freeze({
            master: { host: 'redis-master.local', port: 6379, key: 'redis-master.local:6379' },
            replicas: Object.freeze([])
        })
    })

    await assert.rejects(() => connectTopology(context, createClient), /master failed/)
    assert.deepEqual(calls, ['connect master', 'close master'])
})

test('connectTopology requires discovered topology', async () => {
    var context = createInitialContext('redis+sentinel://s1.local:26379?sentinelMasterId=mymaster')

    await assert.rejects(() => connectTopology(context, () => ({})), /discovered topology/)
})

test('applyTopology returns same context when topology is unchanged', async () => {
    var context = Object.freeze({
        mode: 'sentinel',
        master: { key: 'master:6379', client: {}, status: 'up' },
        replicas: Object.freeze([{ key: 'replica-a:6379', client: {}, status: 'up' }])
    })
    var topology = Object.freeze({
        master: { host: 'master', port: 6379, key: 'master:6379' },
        replicas: Object.freeze([{ host: 'replica-a', port: 6379, key: 'replica-a:6379' }])
    })
    var result = await applyTopology(context, topology, () => {
        throw new Error('should not connect')
    })

    assert.equal(result, context)
})

test('applyTopology connects only added replicas', async () => {
    var calls = []
    var existingReplicaClient = { close: async () => calls.push('close existing replica') }
    var newReplicaClient = {
        connect: async () => calls.push('connect new replica'),
        close: async () => calls.push('close new replica')
    }
    var context = Object.freeze({
        mode: 'sentinel',
        master: { key: 'master:6379', client: {}, status: 'up' },
        replicas: Object.freeze([
            { key: 'replica-a:6379', client: existingReplicaClient, status: 'up' }
        ])
    })
    var topology = Object.freeze({
        master: { host: 'master', port: 6379, key: 'master:6379' },
        replicas: Object.freeze([
            { host: 'replica-a', port: 6379, key: 'replica-a:6379' },
            { host: 'replica-b', port: 6379, key: 'replica-b:6379' }
        ])
    })
    var result = await applyTopology(context, topology, () => newReplicaClient)

    assert.deepEqual(calls, ['connect new replica'])
    assert.equal(result.master, context.master)
    assert.equal(result.replicas.length, 2)
    assert.equal(result.replicas[0], context.replicas[0])
    assert.equal(result.replicas[1].key, 'replica-b:6379')
    assert.equal(result.replicas[1].client, newReplicaClient)
})

test('applyTopology closes removed replicas', async () => {
    var calls = []
    var keepReplicaClient = { close: async () => calls.push('close keep replica') }
    var removedReplicaClient = { close: async () => calls.push('close removed replica') }
    var context = Object.freeze({
        mode: 'sentinel',
        master: { key: 'master:6379', client: {}, status: 'up' },
        replicas: Object.freeze([
            { key: 'replica-a:6379', client: keepReplicaClient, status: 'up' },
            { key: 'replica-b:6379', client: removedReplicaClient, status: 'up' }
        ])
    })
    var topology = Object.freeze({
        master: { host: 'master', port: 6379, key: 'master:6379' },
        replicas: Object.freeze([{ host: 'replica-a', port: 6379, key: 'replica-a:6379' }])
    })
    var result = await applyTopology(context, topology, () => {
        throw new Error('should not connect')
    })

    assert.deepEqual(calls, ['close removed replica'])
    assert.equal(result.replicas.length, 1)
    assert.equal(result.replicas[0], context.replicas[0])
})

test('applyTopology connects new master before closing old master', async () => {
    var calls = []
    var oldMasterClient = { close: async () => calls.push('close old master') }
    var newMasterClient = { connect: async () => calls.push('connect new master') }
    var context = Object.freeze({
        mode: 'sentinel',
        master: { key: 'old-master:6379', client: oldMasterClient, status: 'up' },
        replicas: Object.freeze([])
    })
    var topology = Object.freeze({
        master: { host: 'new-master', port: 6379, key: 'new-master:6379' },
        replicas: Object.freeze([])
    })
    var result = await applyTopology(context, topology, () => newMasterClient)

    assert.deepEqual(calls, ['connect new master', 'close old master'])
    assert.equal(result.master.key, 'new-master:6379')
    assert.equal(result.master.client, newMasterClient)
})

test('applyTopology keeps old master when it becomes a replica', async () => {
    var calls = []
    var oldMasterClient = { close: async () => calls.push('close old master') }
    var newMasterClient = { connect: async () => calls.push('connect new master') }
    var context = Object.freeze({
        mode: 'sentinel',
        master: { key: 'old-master:6379', client: oldMasterClient, status: 'up' },
        replicas: Object.freeze([])
    })
    var topology = Object.freeze({
        master: { host: 'new-master', port: 6379, key: 'new-master:6379' },
        replicas: Object.freeze([{ host: 'old-master', port: 6379, key: 'old-master:6379' }])
    })
    var result = await applyTopology(context, topology, () => newMasterClient)

    assert.deepEqual(calls, ['connect new master'])
    assert.equal(result.master.key, 'new-master:6379')
    assert.equal(result.replicas.length, 1)
    assert.equal(result.replicas[0].role, 'replica')
    assert.equal(result.replicas[0].client, oldMasterClient)
})

test('applyTopology does not close old master when new master connection fails', async () => {
    var calls = []
    var oldMasterClient = { close: async () => calls.push('close old master') }
    var failedNewMasterClient = {
        connect: async () => {
            calls.push('connect new master')
            throw new Error('new master failed')
        },
        close: async () => calls.push('close failed new master')
    }
    var context = Object.freeze({
        mode: 'sentinel',
        master: { key: 'old-master:6379', client: oldMasterClient, status: 'up' },
        replicas: Object.freeze([])
    })
    var topology = Object.freeze({
        master: { host: 'new-master', port: 6379, key: 'new-master:6379' },
        replicas: Object.freeze([])
    })

    await assert.rejects(() => applyTopology(context, topology, () => failedNewMasterClient), /new master failed/)
    assert.deepEqual(calls, ['connect new master', 'close failed new master'])
})

test('reconcileTopology returns same context for unchanged topology', async () => {
    var sentCommands = []
    var context = Object.freeze({
        mode: 'sentinel',
        masterName: 'mymaster',
        sentinel: {
            sendCommand: async (args) => {
                sentCommands.push(args)
                if (args[1] === 'get-master-addr-by-name') return ['master', '6379']
                return [['ip', 'replica-a', 'port', '6379', 'flags', 'slave']]
            }
        },
        master: { key: 'master:6379', client: {}, status: 'up' },
        replicas: Object.freeze([{ key: 'replica-a:6379', client: {}, status: 'up' }])
    })
    var result = await reconcileTopology(context, () => {
        throw new Error('should not connect')
    })

    assert.equal(result, context)
    assert.deepEqual(sentCommands, [
        ['SENTINEL', 'get-master-addr-by-name', 'mymaster'],
        ['SENTINEL', 'replicas', 'mymaster']
    ])
})

test('reconcileTopology applies changed topology', async () => {
    var calls = []
    var newReplicaClient = { connect: async () => calls.push('connect new replica') }
    var context = Object.freeze({
        mode: 'sentinel',
        masterName: 'mymaster',
        sentinel: {
            sendCommand: async (args) => {
                if (args[1] === 'get-master-addr-by-name') return ['master', '6379']
                return [
                    ['ip', 'replica-a', 'port', '6379', 'flags', 'slave'],
                    ['ip', 'replica-b', 'port', '6379', 'flags', 'slave']
                ]
            }
        },
        master: { key: 'master:6379', client: {}, status: 'up' },
        replicas: Object.freeze([{ key: 'replica-a:6379', client: {}, status: 'up' }])
    })
    var result = await reconcileTopology(context, () => newReplicaClient)

    assert.deepEqual(calls, ['connect new replica'])
    assert.equal(result.sentinelStatus, 'up')
    assert.equal(result.lastReconcileError, undefined)
    assert.equal(result.replicas.length, 2)
    assert.equal(result.replicas[1].key, 'replica-b:6379')
})

test('reconcileTopology marks sentinel suspect on discovery failure without closing data plane', async () => {
    var calls = []
    var masterClient = { close: async () => calls.push('close master') }
    var replicaClient = { close: async () => calls.push('close replica') }
    var context = Object.freeze({
        mode: 'sentinel',
        masterName: 'mymaster',
        sentinel: {
            sendCommand: async () => {
                throw new Error('sentinel query failed')
            }
        },
        master: { key: 'master:6379', client: masterClient, status: 'up' },
        replicas: Object.freeze([{ key: 'replica-a:6379', client: replicaClient, status: 'up' }])
    })
    var result = await reconcileTopology(context)

    assert.deepEqual(calls, [])
    assert.equal(result.master, context.master)
    assert.equal(result.replicas, context.replicas)
    assert.equal(result.sentinelStatus, 'suspect')
    assert.match(result.lastReconcileError.message, /sentinel query failed/)
})

test('getReconcileInterval uses option, context option, then default', () => {
    assert.equal(getReconcileInterval({ options: { topologyIntervalMs: 7000 } }, { intervalMs: 1000 }), 1000)
    assert.equal(getReconcileInterval({ options: { topologyIntervalMs: 7000 } }, {}), 7000)
    assert.equal(getReconcileInterval({ options: {} }, {}), 5000)
})

test('createTopologyReconciler starts timer and updates current context', async () => {
    var scheduled = undefined
    var cleared = undefined
    var calls = []
    var scheduler = {
        setInterval: (fn, ms) => {
            scheduled = { fn, ms }
            return 'timer-1'
        },
        clearInterval: timer => {
            cleared = timer
        }
    }
    var context = Object.freeze({
        mode: 'sentinel',
        masterName: 'mymaster',
        options: Object.freeze({ topologyIntervalMs: 1234 }),
        sentinel: {
            sendCommand: async (args) => {
                if (args[1] === 'get-master-addr-by-name') return ['master', '6379']
                return [['ip', 'replica-b', 'port', '6379', 'flags', 'slave']]
            }
        },
        master: { key: 'master:6379', client: {}, status: 'up' },
        replicas: Object.freeze([])
    })
    var newReplicaClient = { connect: async () => calls.push('connect new replica') }
    var reconciler = createTopologyReconciler(context, {
        scheduler,
        createClient: () => newReplicaClient
    })

    assert.equal(scheduled.ms, 1234)
    assert.equal(reconciler.getContext(), context)

    await reconciler.reconcile()

    assert.deepEqual(calls, ['connect new replica'])
    assert.equal(reconciler.getContext().replicas.length, 1)

    reconciler.stop()

    assert.equal(cleared, 'timer-1')
})

test('isSentinelTopologyEvent recognizes relevant sentinel events only', () => {
    assert.equal(isSentinelTopologyEvent('+switch-master'), true)
    assert.equal(isSentinelTopologyEvent('+slave'), true)
    assert.equal(isSentinelTopologyEvent('+sdown'), true)
    assert.equal(isSentinelTopologyEvent('-sdown'), true)
    assert.equal(isSentinelTopologyEvent('+odown'), true)
    assert.equal(isSentinelTopologyEvent('+failover-end'), true)
    assert.equal(isSentinelTopologyEvent('+monitor'), false)
})

test('connectSentinelSubscriber connects separate sentinel subscriber client', async () => {
    var calls = []
    var fakeClient = {
        connect: async () => calls.push('connect subscriber')
    }
    var context = createInitialContext('redis+sentinel://s1.local:26379?sentinelMasterId=mymaster')
    var connected = await connectSentinelSubscriber(context, () => fakeClient)

    assert.deepEqual(calls, ['connect subscriber'])
    assert.equal(connected.sentinelSubscriber, fakeClient)
    assert.equal(connected.sentinel, undefined)
})

test('subscribeSentinelChannels subscribes to topology channels', async () => {
    var subscribed = []
    var callbacks = {}
    var client = {
        subscribe: async (channel, callback) => {
            subscribed.push(channel)
            callbacks[channel] = callback
        }
    }
    var events = []

    await subscribeSentinelChannels(client, (channel, message) => events.push({ channel, message }))

    assert.deepEqual(subscribed, getSentinelTopologyChannels())

    callbacks['+switch-master']('payload')

    assert.deepEqual(events, [{ channel: '+switch-master', message: 'payload' }])
})

test('createDebouncedReconcile collapses multiple schedules into one reconcile', () => {
    var calls = []
    var timers = []
    var cleared = []
    var scheduler = {
        setTimeout: (fn, ms) => {
            var timer = { fn, ms }
            timers.push(timer)
            return timer
        },
        clearTimeout: timer => cleared.push(timer)
    }
    var debounced = createDebouncedReconcile(async () => calls.push('reconcile'), 250, scheduler)

    debounced.schedule()
    debounced.schedule()

    assert.equal(timers.length, 2)
    assert.deepEqual(cleared, [timers[0]])

    timers[1].fn()

    assert.deepEqual(calls, ['reconcile'])
})

test('createSentinelEventSubscription debounces relevant events and ignores unrelated events', async () => {
    var calls = []
    var callbacks = {}
    var timers = []
    var cleared = []
    var subscriberClient = {
        connect: async () => calls.push('connect subscriber'),
        subscribe: async (channel, callback) => {
            callbacks[channel] = callback
        },
        close: async () => calls.push('close subscriber')
    }
    var scheduler = {
        setTimeout: (fn, ms) => {
            var timer = { fn, ms }
            timers.push(timer)
            return timer
        },
        clearTimeout: timer => cleared.push(timer)
    }
    var reconciler = {
        reconcile: async () => calls.push('reconcile')
    }
    var context = createInitialContext('redis+sentinel://s1.local:26379?sentinelMasterId=mymaster')
    var subscription = await createSentinelEventSubscription(context, reconciler, {
        createClient: () => subscriberClient,
        scheduler,
        debounceMs: 50,
        channels: ['+switch-master', '+slave']
    })

    assert.equal(subscription.context.sentinelSubscriber, subscriberClient)
    assert.deepEqual(calls, ['connect subscriber'])

    callbacks['+switch-master']('payload-1')
    callbacks['+slave']('payload-2')

    assert.equal(timers.length, 2)
    assert.deepEqual(cleared, [timers[0]])

    timers[1].fn()

    assert.deepEqual(calls, ['connect subscriber', 'reconcile'])

    await subscription.stop()

    assert.deepEqual(calls, ['connect subscriber', 'reconcile', 'close subscriber'])
})

test('classifyCommand classifies reads, writes, and unknown commands', () => {
    assert.equal(classifyCommand(['GET', 'key']), 'read')
    assert.equal(classifyCommand(['get', 'key']), 'read')
    assert.equal(classifyCommand(['HGETALL', 'hash']), 'read')
    assert.equal(classifyCommand(['XINFO', 'STREAM', 'stream']), 'read')
    assert.equal(classifyCommand(['JSON.GET', 'doc']), 'read')
    assert.equal(classifyCommand(['FT.SEARCH', 'idx', '*']), 'read')
    assert.equal(classifyCommand(['TS.RANGE', 'series', '-', '+']), 'read')
    assert.equal(classifyCommand(['SET', 'key', 'value']), 'write')
    assert.equal(classifyCommand(['EXPIREAT', 'key', '1']), 'write')
    assert.equal(classifyCommand(['ZREMRANGEBYSCORE', 'zset', '0', '1']), 'write')
    assert.equal(classifyCommand(['XGROUP', 'CREATE', 'stream', 'group', '$']), 'write')
    assert.equal(classifyCommand(['JSON.ARRAPPEND', 'doc', '$', '1']), 'write')
    assert.equal(classifyCommand(['FT.CREATE', 'idx', 'SCHEMA', 'name', 'TEXT']), 'write')
    assert.equal(classifyCommand(['TS.ADD', 'series', '*', '1']), 'write')
    assert.equal(classifyCommand(['EVALSHA', 'sha', '0']), 'write')
    assert.equal(classifyCommand(['FLUSHDB']), 'write')
    assert.equal(classifyCommand(['SOMETHING-CUSTOM', 'key']), 'write')
})

test('chooseReplica returns first healthy replica', () => {
    var first = { status: 'down', client: {} }
    var second = { status: 'up', client: { name: 'second' } }
    var third = { status: 'up', client: { name: 'third' } }
    var context = Object.freeze({ replicas: Object.freeze([first, second, third]) })

    assert.equal(chooseReplica(context), second)
})

test('sentinel command routes writes to master', async () => {
    var calls = []
    var masterClient = {
        sendCommand: async (args) => {
            calls.push(['master', args])
            return 'ok'
        }
    }
    var replicaClient = {
        sendCommand: async (args) => {
            calls.push(['replica', args])
            return 'replica'
        }
    }
    var context = Object.freeze({
        mode: 'sentinel',
        master: { status: 'up', client: masterClient },
        replicas: Object.freeze([{ status: 'up', client: replicaClient }])
    })
    var result = await command(['SET', 'key', 'value'], context)

    assert.equal(result, 'ok')
    assert.deepEqual(calls, [['master', ['SET', 'key', 'value']]])
})

test('sentinel command routes reads to healthy replica', async () => {
    var calls = []
    var masterClient = {
        sendCommand: async (args) => {
            calls.push(['master', args])
            return 'master'
        }
    }
    var replicaClient = {
        sendCommand: async (args) => {
            calls.push(['replica', args])
            return 'value'
        }
    }
    var context = Object.freeze({
        mode: 'sentinel',
        master: { status: 'up', client: masterClient },
        replicas: Object.freeze([{ status: 'up', client: replicaClient }])
    })
    var result = await command(['GET', 'key'], context)

    assert.equal(result, 'value')
    assert.deepEqual(calls, [['replica', ['GET', 'key']]])
})

test('sentinel command falls back reads to master when no healthy replica', async () => {
    var calls = []
    var masterClient = {
        sendCommand: async (args) => {
            calls.push(['master', args])
            return 'value'
        }
    }
    var downReplicaClient = {
        sendCommand: async (args) => {
            calls.push(['replica', args])
            return 'replica'
        }
    }
    var context = Object.freeze({
        mode: 'sentinel',
        master: { status: 'up', client: masterClient },
        replicas: Object.freeze([{ status: 'down', client: downReplicaClient }])
    })
    var result = await command(['GET', 'key'], context)

    assert.equal(result, 'value')
    assert.deepEqual(calls, [['master', ['GET', 'key']]])
})

test('sentinel command routes unknown commands to master', async () => {
    var calls = []
    var masterClient = {
        sendCommand: async (args) => {
            calls.push(['master', args])
            return 'ok'
        }
    }
    var replicaClient = {
        sendCommand: async (args) => {
            calls.push(['replica', args])
            return 'replica'
        }
    }
    var context = Object.freeze({
        mode: 'sentinel',
        master: { status: 'up', client: masterClient },
        replicas: Object.freeze([{ status: 'up', client: replicaClient }])
    })
    var result = await command(['CUSTOM.WRITE', 'key'], context)

    assert.equal(result, 'ok')
    assert.deepEqual(calls, [['master', ['CUSTOM.WRITE', 'key']]])
})

test('sentinel command surfaces errors without retry', async () => {
    var attempts = 0
    var masterClient = {
        sendCommand: async () => {
            attempts += 1
            throw new Error('write failed')
        }
    }
    var context = Object.freeze({
        mode: 'sentinel',
        master: { status: 'up', client: masterClient },
        replicas: Object.freeze([])
    })

    await assert.rejects(() => command(['SET', 'key', 'value'], context), /write failed/)
    assert.equal(attempts, 1)
})

test('test real context data from direction connections', async () =>{
    if (!process.env.REDIS_URL) return;
    var context = createInitialContext(process.env.REDIS_URL);
	if(context.mode === 'direct'){
		assert.equal(context.mode, 'direct');
		context = await connectDirect(context);
		assert.equal(context.master.role, 'master');
		await command(['SET', 'foo', 'bar'], context);
		var results = await command(['GET', 'foo'], context);
		assert.notEqual(results, null);
		await closeRedisContext(context);
	}
	assert.equal(1, 1) // just pass
});

test('test real sentinel connections', async ()=>{
	if (!process.env.REDIS_URL) return;
    var context = createInitialContext(process.env.REDIS_URL);
	if(context.mode === 'sentinel'){
		try {
			assert.equal(context.mode, 'sentinel');
			assert.notEqual(context.sentinels.length, 0);
			context = await connectSentinel(context);
			assert.notEqual(context.sentinel, null);
		} finally {
			await closeRedisContext(context);
		}
	}
});

test('test real sentinel topology discovery', async ()=>{
	if (!process.env.REDIS_URL) return;
    var context = createInitialContext(process.env.REDIS_URL);
	if(context.mode === 'sentinel'){
		try {
			context = await connectSentinel(context);
			context = await discoverTopology(context);
			assert.notEqual(context.topology.master, null);
			assert.notEqual(context.topology.master.key, null);
			assert.equal(Array.isArray(context.topology.replicas), true);
		} finally {
			await closeRedisContext(context);
		}
	}
});

test('test real sentinel data node connections', async ()=>{
	if (!process.env.REDIS_URL) return;
    var context = createInitialContext(process.env.REDIS_URL);
	if(context.mode === 'sentinel'){
		try {
			context = await connectSentinel(context);
			context = await discoverTopology(context);
			context = await connectTopology(context);
			assert.equal(context.master.role, 'master');
			assert.equal(context.master.status, 'up');
			assert.equal(Array.isArray(context.replicas), true);
		} finally {
			await closeRedisContext(context);
		}
	}
});

test('test real sentinel command routing', async ()=>{
	if (!process.env.REDIS_URL) return;
    var context = createInitialContext(process.env.REDIS_URL);
	if(context.mode === 'sentinel'){
		try {
			context = await connectSentinel(context);
			context = await discoverTopology(context);
			context = await connectTopology(context);
			await command(['SET', 'phase6-key', 'phase6-value'], context);
			var result = await command(['GET', 'phase6-key'], context);
			assert.notEqual(result, null);
		} finally {
			await closeRedisContext(context);
		}
	}
});

var run = async () => {
    for (var item of tests) {
        try {
            await item.fn()
            console.log(`ok - ${item.name}`)
        } catch (err) {
			console.log(err)
            console.error(`not ok - ${item.name}`)
            throw err
        }
    }
}

run().catch(() => {
    process.exitCode = 1
})
