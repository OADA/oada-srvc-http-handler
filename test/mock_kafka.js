'use strict';

const mock = require('mock-require');

var cbs = {};

before(function mockKafka() {
    mock('kafka-node', {
        Client: function MockClient() {
        },

        Producer: function MockProducer() {
            this.on = function(ev, cb) {
                return ev === 'ready' && setTimeout(cb);
            };
            this.createTopics = function(topics, foo, cb) {
                setTimeout(cb);
            };
            this.send = function mockSend(objs, cb) {
                objs.forEach(function(obj) {
                    [].concat(obj.messages).forEach(function(msg) {
                        setTimeout(() => cbs[obj.topic]({value: msg}));
                    });
                });
                setTimeout(cb);
            };
        },

        ConsumerGroup: function MockConsumerGroup(opts, topics) {
            this.on = function(ev, cb) {
                if (ev === 'message') {
                    topics.forEach(function(topic) {
                        cbs[topic] = cb;
                    });
                }
            };
        },

        Offset: function MockOffset() {
            this.commit = () => undefined;
        }
    });
});
