const pythonShellScript  = require('../helpers').pythonShellScript;
const { json } = require('body-parser');
const express = require('express')
const db = require('../database')
const router = express.Router();



// test route
router.get('/', (req, res) => {
    res.json({toto:"models"})
});



//call a model ( quantitymodel)
router.post("/quantitymodel",(req,res)=>{

    const date_debut = req.body.date_debut
    const date_fin = req.body.date_fin
   
    var options = {
      //scriptPath: '',
      //replace this dates with the ones you will receive from req.body
      args: [date_debut,date_fin],
    };
    const path = 'models/quantity_model.py'
    try{
      pythonShellScript(path , options)
      res.status(200).json({msg:"the model has been called successfully"})
    } catch (err) {
      res.send(err)
    }

  
  //either contact your model again like this 
  //   const options2={
  //     scriptPath:'',
  //     args:["updated_db"]
  //   }
  // PythonShell.run('models/model.py', options2, function (err, results) {
  //   if (err) throw err;
  //     //and you will have the results here in the results variable
  //   console.log('results: %j', results);
  // });
  //   //return it this way
  //   res.json({...results})
  //
    //or contact the cassandra db from here and return the results in the same way res.json({...results})
  })


module.exports = router 
