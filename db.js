'use strict';

// TODO: Replace this with abalmos library?

const arangojs = require('arangojs');

var db = arangojs({
    url: 'http://arango:8529'
});

exports.getResource = function getResource(id, path) {
    // TODO: Escaping stuff?
    path = (path||'')
        .split('/')
        .filter(x => !!x)
        .join('"]["');
    path = path ? '["' + path + '"]' : '';

    return db.query(arangojs.aql`
        RETURN DOCUMENT(resources/${id})${path}
    `);
};

exports.setResource = function setResource(id, path, val) {
    // TODO: Wait for abalmos library to implement this
}
