const express = require('express')
const db = require('../database')
const router = express.Router();

// test request 
router.get('/' , (req, res) => {
    res.json({toto:"historiqueRoute"})
})
const cassandra = require('cassandra-driver');
const { request } = require('express');

const client = new cassandra.Client({
  contactPoints: ['127.0.0.1'],
  localDataCenter: 'datacenter1',
  keyspace: 'user'
});


// get all the historiy ( don't forget to define the feilds of the history table )
router.get('/AllHistories', (req, res) =>{
    const query = 'SELECT * FROM user';    
    try{
        
            client.execute(query, function (err, result) {
            var histoty = result
            //The row is an Object with column names as property keys. 
            res.status(200).send(histoty?.rows);
            });     
    }catch(err){
        console.log(err);
    }

})


// get all the historiy ( don't forget to define the feilds of the history table )
router.get('/OneHistory/:date', (req, res) =>{
    const date = req.params.date;

    const query = 'SELECT * FROM user WHERE  username= ?  ALLOW FILTERING ';      
    client.execute(query, [date]).then(result => {
        console.log(result);
            var oneHistory = result
            //The row is an Object with column names as property keys. 
            res.status(200).send(oneHistory?.rows);
    }).catch((err) => {console.log('ERROR :', err);});
});
        
       
module.exports = router;