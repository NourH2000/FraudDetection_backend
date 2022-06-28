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
  keyspace: 'fraud'
});


// get all the historiy Of quantityResult
router.get('/AllquantityResult', (req, res) =>{
    const query = 'SELECT * FROM Quantity_result';    
    try{
        
            client.execute(query, function (err, result) {
            var history = result
            //The row is an Object with column names as property keys. 
            res.status(200).send(history?.rows);
            });     
    }catch(err){
        console.log(err);
    }

})


// get all the history by idEntrainement
router.get('/OneQuantityHistoryByID/:idEntrainement', (req, res) =>{
    const idEntrainement = req.params.idEntrainement;

    const query = 'SELECT * FROM Quantity_result WHERE  id_entrainement= ?  ALLOW FILTERING ';      
    client.execute(query, [idEntrainement] , {prepare: true},).then(result => {
        console.log(result);
            var oneHistoryById = result
            //The row is an Object with column names as property keys. 
            res.status(200).send(oneHistoryById?.rows);
    }).catch((err) => {console.log('ERROR :', err);});
});


// get all the history by NumEnR
router.get('/OneQuantityHistoryByNumEnr/:NumEnR', (req, res) =>{
    const NumEnR = req.params.NumEnR;

    const query = 'SELECT * FROM Quantity_result WHERE  num_enr= ?  ALLOW FILTERING ';      
    client.execute(query, [NumEnR] , {prepare: true},).then(result => {
        console.log(result);
            var oneHistoryByNum_Enr = result
            //The row is an Object with column names as property keys. 
            res.status(200).send(oneHistoryByNum_Enr?.rows);
    }).catch((err) => {console.log('ERROR :', err);});
});


// get all the history by NumEnR and idEntrainement
router.get('/OneQuantityHistoryByNumEnr/:NumEnR/:idEntrainement', (req, res) =>{
    const NumEnR = req.params.NumEnR;
    const idEntrainement = req.params.idEntrainement;

    const query = 'SELECT * FROM Quantity_result WHERE  num_enr= ? AND id_entrainement= ?  ALLOW FILTERING ';      
    client.execute(query, [NumEnR ,idEntrainement ] , {prepare: true},).then(result => {
        console.log(result);
            var oneHistoryByNum_EnrAndIdEntrainement = result
            //The row is an Object with column names as property keys. 
            res.status(200).send(oneHistoryByNum_EnrAndIdEntrainement?.rows);
    }).catch((err) => {console.log('ERROR :', err);});
});

// get all the history by Date
router.get('/OneQuantityHistoryByDate/:date', (req, res) =>{
    const date = req.params.date;

    const query = 'SELECT * FROM Quantity_result WHERE  date_entrainement= ?  ALLOW FILTERING ';      
    client.execute(query, [date] , {prepare: true},).then(result => {
        console.log(result);
            var oneHistoryByDate = result
            //The row is an Object with column names as property keys. 
            res.status(200).send(oneHistoryByDate?.rows);
    }).catch((err) => {console.log('ERROR :', err);});
});




// get all the History Of Training
router.get('/HistoryOfTraining', (req, res) =>{
    const query = 'SELECT * FROM HistoryTrainings';    
    try{
        
            client.execute(query, function (err, result) {
            var historyOfTraining = result
            //The row is an Object with column names as property keys. 
            res.status(200).send(historyOfTraining?.rows);
            });     
    }catch(err){
        console.log(err);
    }

})

// get all the History Of Quantity_result groupedBy num_enr
router.get('/QuantityResultGroupedByNumEnr', (req, res) =>{
    const query = 'select * from Quantity_result group by num_enr ALLOW FILTERING';    
    try{
        
            client.execute(query, function (err, result) {
            var ResultGroupedByNumEnr = result
            //The row is an Object with column names as property keys. 
            res.status(200).send(ResultGroupedByNumEnr?.rows);
            });     
    }catch(err){
        console.log(err);
    }

})

// get all the History Of Quantity_result groupedBy num_enr and idEntrainement
router.get('/QuantityResultGroupedByNumEnrAndId/:idEntrainement', (req, res) =>{

    const query = 'select * from Quantity_result where id_entrainement = ? group by num_enr ALLOW FILTERING ;';    
    const idEntrainement = req.params.idEntrainement;

    client.execute(query, [idEntrainement] , {prepare: true},).then(result => {
        console.log(result);
            var ResultGroupedByNumEnrAndID = result
            //The row is an Object with column names as property keys. 
            res.status(200).send(ResultGroupedByNumEnrAndID?.rows);
    }).catch((err) => {console.log('ERROR :', err);});
});



// get the data from quantityResult groupedBy Num_EnR( medicament ) #####################"# NOt YET
router.get("/GetAllByMedicament",(req,res)=>{

    const query = 'SELECT *  FROM Quantity_result  GROUP BY num_enr'; 
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


  
       
module.exports = router;