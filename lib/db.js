"use strict";

var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;
var Q = require('q');
var fs = require('fs');
var co = require('co');
var _ = require('lodash');
var db;

module.exports = {
  getConnection: getConnection
};

function getURI(config) {
  if (!config.uri) {
    throw 'Expected `uri` option is not specified';
  }

  return config.uri;
}

function getOptions(options) {
  return co(function * () {
    var mongos = options.mongos || {},
      sslCA = mongos.sslCA || [];

    if (typeof(sslCA) === 'string') {
      mongos.sslCA = [sslCA];
    }

    //replace file paths with file data
    mongos.sslCA = yield _.map(sslCA, function (sslCA) {
      return Q.nfcall(fs.readFile, sslCA);
    });

    return options;
  });
}

function connect(config) {
  return co(function * () {
    let uri = getURI(config);
    let options = yield getOptions(config.options || {});
    db = yield MongoClient.connect(uri, options);

    return db;
  });
}

function getConnection(config, cb) {
  co(function *() {
    if (!db) {
      db = yield connect(config);
    }

    cb(null, {
      connection: db,
      migrationCollection: db.collection('migrations')
    });

  }).catch(function (err) {
    console.error('Failed to connect to mongodb (native driver)');
    console.error(err.stack || err);
    cb(err);
  });
}
