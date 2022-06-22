//const pythonShellScript  = require('../helpers').pythonShellScript; and delete the next line
var { PythonShell } = require('python-shell');

const { json } = require('body-parser');
const express = require('express')
const db = require('../database')
const router = express.Router();

//data base connection
const cassandra = require('cassandra-driver');
const { request } = require('express');
const client = new cassandra.Client({
  contactPoints: ['127.0.0.1'],
  localDataCenter: 'datacenter1',
  keyspace: 'fraud'
});



// test route
router.get('/', (req, res) => {
    res.json({toto:"models"})
});



//call a model ( quantitymodel)
router.post("/quantitymodel",(req,res)=>{

    const date_debut = req.body.date_debut
    const date_fin = req.body.date_fin
    const query = 'MAX(id_entrainement) FROM quantity_result LIMIT 1 ;';  
   
    var options = {
      //scriptPath: '',
      //replace this dates with the ones you will receive from req.body
      args: [date_debut,date_fin],
    };
    const path = 'models/quantity_model.py'
    try{

      PythonShell.run(path, options, function (err, results) {
        if (err) throw err;
        //during execution of script.
         //console.log('result: ', results);
         res.json({...results})
         
////////////////////////////////////

         try{client.execute(query, function (err, result) {
          var max = result
          //The row is an Object with column names as property keys. 
          console.log(max?.rows);
          });}
          catch (err) {console.log(err.message);}
////////////////////////////////////
         
        
      });
    } catch (err) {
      res.send(err)
    }

  })


module.exports = router 
