const pythonShellScript  = require('../helpers').pythonShellScript;
const express = require('express')
const db = require('../database')
const router = express.Router();



// test route
router.get('/', (req, res) => {
    res.json({toto:"models"})
});


router.post('/posts', (req, res) => {
    
    console.log(req.body)
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
    const path = 'models/model.py'
    pythonShellScript(path , options)

  
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
