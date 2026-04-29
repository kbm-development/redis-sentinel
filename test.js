'use strict'

var assert = require('node:assert/strict')
var {
    parseRedisUrl,
    createInitialContext,
    parseSentinelNode,
    parsePort
} = require('./index')

var test = (name, fn) => {
    try {
        fn()
        console.log(`ok - ${name}`)
    } catch (err) {
        console.error(`not ok - ${name}`)
        throw err
    }
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

test('createInitialContext from env REDIS_URL', ()=>{
    var context = createInitialContext(process.env.REDIS_URL);
    assert.equal(context.mode, 'sentinel')
    assert.equal(context.masterName, 'mymaster')
    assert.equal(context.sentinel, undefined)
    assert.equal(context.sentinelSubscriber, undefined)
    assert.deepEqual(context.replicas, [])
    assert.deepEqual(context.timers, {})
})
