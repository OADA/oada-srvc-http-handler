'use strict';

var Promise = require('bluebird');
var express = require('express');
var express_promise = require('express-promise');
var uuid = require('uuid');
//var _ = require('lodash');
var body_parser = require('body-parser');
//var bunyan = require('bunyan');
//var content_type_parser = require('content-type');
var cors = require('cors');
var fs = require('fs');
var https = require('https');
var well_known_json = require('well-known-json');
var oada_error = require('oada-error');
//var oada_ref_auth = require('oada-ref-auth');
const kf = require('kafka-node');


// Local libs:
/*
var bookmarks_handler = config.libs.handlers.bookmarks();
var resources_handler = config.libs.handlers.resources();
var meta_handler = config.libs.handlers.meta();
var mediatype_parser = config.libs.mediatype_parser();
var errors = config.libs.error();
var log = config.libs.log();
*/
var log = console;
var client = Promise.promisifyAll(new kf.Client("zookeeper:2181","http-handler"));
var offset = Promise.promisifyAll(new kf.Offset(client));
var producer = Promise.promisifyAll(new kf.Producer(client, {
    partitionerType: 0 //kf.Producer.PARTITIONER_TYPES.keyed
}));
var consumer = Promise.promisifyAll(new kf.ConsumerGroup({
    host: 'zookeeper:2181',
    groupId: 'http-handlers',
    fromOffset: 'latest'
}, ['http_response']));

producer = producer
    .onAsync('ready')
    .return(producer)
    .tap(function(prod) {
        return prod.createTopicsAsync(['token_request'], true);
    });

var requests = {};
consumer.on('message', function(msg) {
    var resp = JSON.parse(msg.value);

    var done = requests[resp.connection_id];

    offset.commit(msg);
    
    return done && done(null, resp);
});

var _server = {
    app: null,

    // opts.nolisten = true|false // used mainly for testing
    start: function(opts) {
      return Promise.try(function() {
        opts = opts || {};
        log.info('-------------------------------------------------------------');
        log.info('Starting server...');
      }).then(function() {

        /////////////////////////////////////////////////////////////////
        // Setup express:
        _server.app = express();

        // Allow route handlers to return promises:
        _server.app.use(express_promise());

        // Log all requests before anything else gets them for debugging:
        _server.app.use(function(req, res, next) {
          log.info('Received request: ' + req.method + ' ' + req.url);
          log.trace('req.headers = ', req.headers);
          log.trace('req.body = ', req.body);
          next();
        });

        // Turn on CORS for all domains, allow the necessary headers
        _server.app.use(cors({
          exposedHeaders: [ 'x-oada-rev', 'location' ],
        }));
        _server.app.options('*', cors());

        ////////////////////////////////////////////////////////
        // Configure the OADA well-known handler middleware
        var well_known_handler = well_known_json({
          headers: {
            'content-type': 'application/vnd.oada.oada-configuration.1+json',
          },
        });
          //well_known_handler.addResource('oada-configuration', config.oada_configuration);
        _server.app.use(well_known_handler);

        // Enable the OADA Auth code to handle OAuth2
        /*
        _server.app.use(oada_ref_auth({
          wkj: well_known_handler,
          server: config.server,
          datastores: _.mapValues(config.libs.auth.datastores, function(d) { return d(); }), // invoke each config
        }));
        */
        _server.app.use(function requestId(req, res, next) {
            req.id = uuid();
            next();
        });

        _server.app.use(function tokenHandler(req, res, next) {
            var reqDone = Promise.fromCallback(function(done) {
                requests[req.id] = done;
            });
            producer.then(function sendTokReq(prod) {
                return prod.sendAsync([{
                    topic: 'token_request',
                    messages: JSON.stringify({
                        'token': req.get('authorization'),
                        'resp_partition': 0, // TODO: Handle partitions
                        'connection_id': req.id
                    })
                }]);
            })
            .then(function waitTokRes() {
                return reqDone.timeout(1000);
            })
            .then(function handleTokRes(resp) {
                req.user = resp;
            })
            .finally(function cleanupTokReq() {
                delete requests[req.id];
            })
            .asCallback(next);
        });

        _server.app.use(function graphHandler(req, res, next) {
            console.log(req.url);
        });

        /////////////////////////////////////////////////////////////////
        // Setup the body parser and associated error handler:
        _server.app.use(body_parser.raw({
          limit: '10mb',
          type: function(req) {
            return mediatype_parser.canParse(req);
          }
        }));

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
        _server.app.use(function(req, res){
          throw new oada_error.OADAError('Route not found: ' + req.url, oada_error.codes.NOT_FOUND);
        });

        ///////////////////////////////////////////////////
        // Use OADA middleware to catch errors and respond
        _server.app.use(oada_error.middleware(console.log));


        ///////////////////////////////////////////////////
        // Testing libraries can disable the actual listening
        // on a port by passing opts.nolisten to start()
        if (!opts.nolisten) {
          // Set the port and start the server (HTTPS vs. HTTP)
          //_server.app.set('port', config.server.port);
          _server.app.set('port', 80);
          if(false && config.server.protocol === 'https://') {
            var s = https.createServer(config.server.certs, _server.app);
            s.listen(_server.app.get('port'), function() {
              log.info('OADA Test Server started on port ' + _server.app.get('port')
                  + ' [https]');
            });
          } else {
            _server.app.listen(_server.app.get('port'), function() {
              log.info('OADA Test Server started on port ' + _server.app.get('port'));
            });
          }
        }
      });
    },
};

_server.start();

