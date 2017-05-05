'use strict';

const Promise = require('bluebird');
const expect = require('chai').expect;
const request = require('supertest');
var kf;
var app;
var consumer;
var client;
var producer;


describe('GET /bookmarks/a', function() {
    const token = 'Bearer FOOBAR';
    const user = '123';
    const bookmarks = '/resources/123';
    const scope = 'asadasdsads';
    var id;
    var req;

    before(function() {
        kf = require('kafka-node');
        app = require('../').app;

        consumer = Promise.promisifyAll(new kf.ConsumerGroup({
            host: 'zookeeper:2181',
            groupId: 'test',
            fromOffset: 'latest'
        }, ['token_request', 'graph_request']));

        client = Promise.promisifyAll(new kf.Client("zookeeper:2181","http-handler"));

        producer = Promise.promisifyAll(new kf.Producer(client, {
            partitionerType: 0 //kf.Producer.PARTITIONER_TYPES.keyed
        }));
        producer = producer
            .onAsync('ready')
            .return(producer)
            .tap(function(prod) {
                return prod.createTopicsAsync(['token_request'], true);
            });
    });

    step('should make token_request', function() {
        var resp = Promise.fromCallback(done => {
            consumer.on('message', msg => {
                done(null, msg);
            });
        });
        req = request(app)
            .get('/bookmarks/a')
            .set('Authorization', token)
            .then(() => {});

        return resp
            .get('value')
            .then(JSON.parse)
            .then(resp => {
                id = resp.connection_id;
                expect(resp.token).to.equal(token);
            });
    });

    step('should resolve bookmarks', function() {
        var resp = Promise.fromCallback(done => {
            consumer.on('message', msg => {
                done(null, msg);
            });
        });
        return producer
            .then(function(prod) {
                return prod.sendAsync([{
                    topic: 'http_response',
                    messages: JSON.stringify({
                        'connection_id': id,
                        token: token,
                        doc: {
                            'user_id': user,
                            'bookmarks_id': bookmarks,
                            'scope': scope
                        }
                    })
                }]);
            })
            .then(() => {
                return resp
                    .get('value')
                    .then(JSON.parse)
                    .then(resp => {
                        expect(resp.url).to.match(RegExp('^' + bookmarks));
                    });
            });
    });

    step('should respond with document', function() {
        return producer
            .then(function(prod) {
                return prod.sendAsync([{
                    topic: 'http_response',
                    messages: JSON.stringify({
                        'connection_id': id,
                        'token': token,
                        'url': '/resources/123/a',
                        'resouce_id': '123',
                        'path_leftover': 'a/',
                        'meta_id': '456'
                    })
                }]);
            })
            .then(() => req);
    });
});
