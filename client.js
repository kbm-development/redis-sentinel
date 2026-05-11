var redis = require('./index')

var isString = (value) => typeof value === 'string'
var isObject = (value) => typeof value === 'object' && value !== null && !Array.isArray(value);
var isFn = (value) => typeof value === 'function'

var first = (items) => items[0]
var rest = (items) => items.slice(1)
var peek = (items) => items[items.length - 1]
var pop = (items) => items.slice(0, -1);
var map = (...args) =>{
  let [fn, arr] = args;
  if (args.length === 1) {
    return coll => map(fn, coll);
  }
  return arr.map(fn);
}

var concat = (...items) => [].concat(...items)
var lowerCase = (value) => String(value).toLowerCase()
var toString = (value) => String(value);

var seq = (arg) =>{
  if(Array.isArray(arg)){
    return arg;
  }
  if(typeof arg === "object"){
    return Object.entries(arg);
  }
  if(typeof arg === "string"){
    return Array.from(arg);
  }
  return arg;
}

var flatten =(...args) => {
  let [arr, level] = args;
  if(args.length === 1){
    level = Infinity;
  }
  return arr.flat(level);
};

var merge = (...args) => {
  let [obj1, obj2] = args;
  if(args.length === 1) return (obj1) => merge(obj1, obj2);
  return Object.assign({}, ...args);
}

var stringify = (data) => {
  return isObject(data) ? JSON.stringify(data) : (!isString(data) ? data.toString() : data);
}

var parseData = (data) => {
  if (isObject(data)) return map(toString, flatten(seq(data)))
  return data;
}

var commandType = {
    'json.set': (args) => {
        var [_, key, path, value] = args
        return ['JSON.SET', key, path, stringify(value)]
    },

    'json.get': (args) => {
        var [_, key, ...path] = args
        return path.length ? concat(['JSON.GET', key], path) : ['JSON.GET', key]
    },

    'json.mget': (args) => {
        var path = peek(args) || '$'
        var keys = pop(rest(args))
        return concat(['JSON.MGET'], keys, path)
    },

    'xadd': (args) => {
        var [_, key, data, length] = args
        if (length) return concat(['XADD', key, 'MAXLEN', toString(length), '*'], parseData(data))
        return concat(['XADD', key, '*'], parseData(data))
    },

    'xread': (args) => {
        var [_, key, count = '1', timeout = '0', start = '0'] = args
        return ['XREAD', 'COUNT', count, 'BLOCK', timeout, 'STREAMS', key, start]
    },

    'xreadgroup': (args) => {
        var [_, key, group, consumer, count = '1', timeout = '0', start = '>'] = args
        return [
            'XREADGROUP', 'GROUP', group, consumer,
            'COUNT', count, 'BLOCK', timeout, 'STREAMS', key, start
        ]
    },

    'xgroup': (args) => {
        var [_, key, group, target = '$'] = args
        return ['XGROUP', 'CREATE', key, group, target, 'MKSTREAM']
    }
}

var transformCommand = (commands) => {
    var resolve = commandType[lowerCase(first(commands))]
    if (resolve) return resolve(commands)
    return commands
}


var parseResult = (type, command) => (result) => {
  if(!result) return result;
  if(type === 'json.get'){
    try{
      let res = JSON.parse(result);
      return res;
    }catch(err){
      console.log('Unable to parse result');
      return {};
    }    
  }
  if(type === 'json.mget'){
    return map((r) => {
      if(r){
        if(peek(command) === '$') return first(JSON.parse(r));
        return JSON.parse(r);
      }
      return r;
    }, result);
  }
  return result;
};

var redisUri = (uri) => {
	if(uri.includes('redis+sentinel://')) return uri;
	if(uri.includes(',')) return uri.split(',')[0];
	return uri;
};

var isPromise = (obj) => {
  return !!obj && (typeof obj === 'object' || typeof obj === 'function') && typeof obj.then === 'function';
};

var sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

var ack = (key, group, rs) => (ids) => {
    return redis.command(['XACK', key, group].concat(ids), rs)
}

var command = async (...args) =>{
    var [commands, client, forceMode] = args
    if (args.length === 1) return (nextClient) => command(commands, nextClient)
    if (isFn(client)) client = client()
    var type = lowerCase(first(commands))
    var adapted = transformCommand(commands)
	return redis.command(adapted, client).then(parseResult(type, adapted));
};


module.exports = { command, createRedis: redis.createRedis, connectRedis: redis.connectRedis };
