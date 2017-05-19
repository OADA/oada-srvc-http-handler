'use strict';

var Promise = require('bluebird');
const oadaLib = require('oada-lib-arangodb');
const express = require('express');
const expressPromise = require('express-promise');
const uuid = require('uuid');
const bodyParser = require('body-parser');
const cors = require('cors');
const wellKnownJson = require('well-known-json');
const oadaError = require('oada-error');
const OADAError = oadaError.OADAError;
const kf = require('kafka-node');
const debug = require('debug')('http-handler');

var config = require('./config');

var client = new kf.Client(config.get('kafka:broker'), 'http-handler');
var offset = Promise.promisifyAll(new kf.Offset(client));
var producer = Promise.promisifyAll(new kf.Producer(client, {
    partitionerType: 0 //kf.Producer.PARTITIONER_TYPES.keyed
}));
var consumer = new kf.ConsumerGroup({
    host: config.get('kafka:broker'),
    groupId: 'http-handlers',
    fromOffset: 'latest'
}, [config.get('kafka:topics:httpResponse')]);

producer = producer
    .onAsync('ready')
    .return(producer);

var requests = {};
consumer.on('message', function(msg) {
    var resp = JSON.parse(msg.value);

    var done = requests[resp['connection_id']];

    offset.commit('http-handlers', [msg]);

    return done && done(null, resp);
});
// Produce request, cosume response, then resolve to answer
function kafkaRequest(id, topic, message) {
    var reqDone = Promise.fromCallback(function(done) {
        requests[id] = done;
    });
    message = Object.assign({}, message, {
        'connection_id': id,
        'resp_partition': 0, // TODO: Handle partitions
    });

    return producer.then(function sendKafkaReq(prod) {
        return prod.sendAsync([{
            topic: topic,
            messages: JSON.stringify(message)
        }]);
    })
    .then(function waitKafkaRes() {
        return reqDone.timeout(1000, topic + ' timeout');
    })
    .finally(function cleanupKafkaReq() {
        delete requests[id];
    });
}

var _server = {
    app: null,

    // opts.nolisten = true|false // used mainly for testing
    start: function() {
        return Promise.fromCallback(function(done) {
            debug('----------------------------------------------------------');
            debug('Starting server...');

            _server.app.set('port', config.get('server:port'));
            _server.app.listen(_server.app.get('port'), done);
        })
        .tap(() => {
            debug('OADA Test Server started on port ' +
                    _server.app.get('port'));
        });
    },
};

/////////////////////////////////////////////////////////////////
// Setup express:
_server.app = express();

// Allow route handlers to return promises:
_server.app.use(expressPromise());

// Log all requests before anything else gets them for debugging:
_server.app.use(function(req, res, next) {
    debug('Received request: ' + req.method + ' ' + req.url);
    debug('req.headers = ', req.headers);
    debug('req.body = ', req.body);
    next();
});

// Turn on CORS for all domains, allow the necessary headers
_server.app.use(cors({
    exposedHeaders: ['x-oada-rev', 'location'],
}));
_server.app.options('*', cors());

////////////////////////////////////////////////////////
// Configure the OADA well-known handler middleware
var wellKnownHandler = wellKnownJson({
    headers: {
        'content-type': 'application/vnd.oada.oada-configuration.1+json',
    },
});
//wellKnownHandler.addResource('oada-configuration', config.oada_configuration);
_server.app.use(wellKnownHandler);

// Enable the OADA Auth code to handle OAuth2
/*
_server.app.use(oada_ref_auth({
    wkj: wellKnownHandler,
    server: config.server,
    datastores: _.mapValues(config.libs.auth.datastores, function(d) {
        return d(); // invoke each config
    }),
}));
*/
_server.app.use(function requestId(req, res, next) {
    req.id = uuid();
    next();
});

_server.app.use(function tokenHandler(req, res, next) {
    return kafkaRequest(req.id, config.get('kafka:topics:tokenRequest'), {
        'token': req.get('authorization'),
    })
    .tap(function checkTok(tok) {
        if (!tok['token_exists']) {
            throw new OADAError('Unauthorized', 401);
        }
    })
    .then(function handleTokRes(resp) {
        req.user = resp;
    })
    .finally(function cleanupTokReq() {
        delete requests[req.id];
    })
    .asCallback(next);
});

// Rewrite the URL if it starts with /bookmarks
_server.app.use(function handleBookmarks(req, res, next) {
    req.url = req.url.replace(/^\/bookmarks/,
      `/resources/${req.user.doc['bookmarks_id']}`);
    next();
});

_server.app.use(function graphHandler(req, res, next) {
    return kafkaRequest(req.id, config.get('kafka:topics:graphRequest'), {
        'token': req.get('authorization'),
        'url': req.url,
    })
    .then(function handleGraphRes(resp) {
        req.url = `/resources/${resp['resource_id']}`;
        // TODO: Just use express parameters rather than graph thing?
        req.oadaGraph = resp;
    })
    .asCallback(next);
});

/////////////////////////////////////////////////////////////////
// Setup the body parser and associated error handler:
_server.app.use(bodyParser.json({
    limit: '10mb',
}));

// TODO: Is this scope stuff right/good?
function checkScopes(scope, contentType) {
    const scopeTypes = {
        'oada.rocks': [
            'application/vnd.oada.bookmarks.1+json',
            'application/vnd.oada.rocks.1+json',
            'application/vnd.oada.rock.1+json',
        ]
    };
    function scopePerm(perm, has) {
        return perm === has || perm === 'all';
    }

    return scope.some(function chkScope(scope) {
        var type;
        var perm;
        [type, perm] = scope.split(':');

        if (!scopeTypes[type]) {
            debug('Unsupported scope type "' + type + '"');
            return false;
        }

        return scopeTypes[type].indexOf(contentType) >= 0 &&
                scopePerm(perm, 'read');
    });
}
_server.app.get('/resources/*', function getResource(req, res, next) {
    // TODO: Should it not get the whole meta document?
    // TODO: Make getResource accept an array of paths and return an array of
    //       results. I think we can do that in one arango query
    var owned = oadaLib.resources
        .getResource(req.oadaGraph['resource_id'], '_meta/_owner')
        .call('next')
        .then(function checkOwner(owner) {
            if (owner !== req.user.doc['user_id']) {
                throw new OADAError('Not Authorized', 403);
            }
        });

    var scoped = oadaLib.resources
        .getResource(req.oadaGraph['resource_id'], '_meta/_type')
        .call('next')
        .then(checkScopes.bind(null, req.user.doc.scope))
        .then(function scopesAllowed(allowed) {
            if (!allowed) {
                throw new OADAError('Not Authorized', 403);
            }
        });

    var doc = oadaLib.resources.getResource(
            req.oadaGraph['resource_id'],
            req.oadaGraph['path_leftover']
    );

    return Promise
        .join(doc, owned, scoped, function(doc) {
            return doc.next();
        })
        .then(function returnDoc(doc) {
            if (!doc) {
                throw new OADAError('Not Found', 404);
            }

            return res.json(unflattenMeta(doc));
        })
        .catch(next);
});

// TODO: This was a quick make it work. Do what you want with it.
function unflattenMeta(doc) {
    Object.keys(doc).forEach((key) => {
        if (doc[key]._id) {
            if (doc[key]['_oada_rev']) {
                doc[key] = {
                    '_id': doc[key]._id,
                    '_oada_rev': doc[key]['_oada_rev']
                };
            } else {
                doc[key] = {_id: doc[key]._id};
            }
        } else {
            if (typeof doc[key] === 'object') {
                doc[key] = unflattenMeta(doc[key]);
            }
        }
    });

    return doc;
}

_server.app.post('/resources/*', function postResource(req, res, next) {
    // Turn POST into PUT at random id
    req.url += '/' + uuid(); // TODO: Is this a good way to generate new id?
    req.method = 'PUT';

    next();
});

_server.app.put('/resources/*', function putResource(req, res, next) {
    if (!checkScopes(req.user.doc.scope, req.get('Content-Type'))) {
        return next(new OADAError('Not Authorized', 403));
    }

    return kafkaRequest(req.id, config.get('kafka:topics:writeRequest'), {
        url: req.url,
        'resource_id': req.oadaGraph['resource_id'],
        'path_leftover': req.oadaGraph['path_leftover'],
        'meta_id': req.oadaGraph['meta_id'],
        'user_id': req.user.doc['user_id'],
        'client_id': req.user.doc['client_id'],
        'content_type': req.get('Content-Type'),
        body: req.body
    })
    .tap(function checkWrite(resp) {
        if (resp.code !== 'success') {
            var err = new OADAError('write failed with code ' + resp.code);
            return Promise.reject(err);
        }
    })
    .then(function(resp) {
        return res
            .set('X-OADA-Rev', resp['_rev'])
            .location('/resources/' + resp['resource_id'])
            .sendStatus(204);
    })
    .catch(next);
});

/////////////////////////////////////////////////////////
// Setup the resources, meta, and bookmarks routes:

// NOTE: must register bookmarks_handler and meta_handler prior to
// resources_handler because they call next() to get to the
// resources handler.
/*
_server.app.use(config.server.path_prefix, bookmarks_handler);
_server.app.use(config.server.path_prefix, meta_handler);
_server.app.use(config.server.path_prefix, resources_handler);
*/

//////////////////////////////////////////////////
// Default handler for top-level routes not found:
_server.app.use(function(req) {
    throw new oadaError
        .OADAError('Route not found: ' + req.url, oadaError.codes.NOT_FOUND);
});

///////////////////////////////////////////////////
// Use OADA middleware to catch errors and respond
_server.app.use(oadaError.middleware(debug));

if (require.main === module) {
    _server.start();
}
module.exports = _server;

