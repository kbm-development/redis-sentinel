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

test('test real context data from direction connections', async () =>{
    if (!process.env.REDIS_URL) return;
    var context = createInitialContext(process.env.REDIS_URL);
	if(context.mode === 'direct'){
		assert.equal(context.mode, 'direct');
		context = await connectDirect(context);
		assert.equal(context.master.role, 'master');
		await command(['SET', 'foo', 'bar'], context);
		let results = await command(['GET', 'foo'], context);
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
