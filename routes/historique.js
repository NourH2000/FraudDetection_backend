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
router.get('/allHistories', (req, res) =>{
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
router.get('/oneHistory/:date', (req, res) =>{
    const date = req.params.date;
    console.log(date);
    const query = "SELECT * FROM user where  username= ? ";    
    try{
        
        client.execute(query,['date'], function (err, result) {
            console.log(result);
            var Onehistoty = result
            //The row is an Object with column names as property keys. 
            res.status(200).send(Onehistoty?.rows);
            
            
          });     
    }catch(err){
        console.log(err);
    }

 })

module.exports = router;