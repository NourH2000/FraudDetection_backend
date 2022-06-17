const express = require('express')
const app = express()

const cassandra = require('cassandra-driver');

const client = new cassandra.Client({
  contactPoints: ['127.0.0.1'],
  localDataCenter: 'datacenter1',
  keyspace: 'user'
});

const query = 'SELECT * FROM user';
 
client.execute(query)
  .then(result => console.log('User with username %s', result.rows[1].username));

/***************************************************************************************************************** */

var { PythonShell } = require('python-shell');
var options = {
    scriptPath: '',
    args: ["2022-01-16","2022-01-18"],
};

PythonShell.run('models/model.py', options, function (err, results) {
  if (err) throw err;
  console.log('results: %j', results);
});