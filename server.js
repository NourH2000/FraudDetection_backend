const express = require('express')
const session = require("express-session");
const cors = require("cors");
const cookieParser = require("cookie-parser");
const bodyParser = require("body-parser");



// les routes 
const authRouter = require('./routes/auth')
const bodyParser = require("body-parser");

app.use(session({
  secret  : 'secret_code',
  resave: true,
  saveUninitialized: true,
}))

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

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

//routes
app.get("/",(req,res)=>{
  //this is just a test route
  res.json({toto:"ioi"})
})
app.post("/fromtotraining",(req,res)=>{

  date_debut = req.body.debut
  date_debut = req.body.fin
var options = {
    scriptPath: '',
    //replace this dates with the ones you will receive from req.body
    args: [date_debut,date_debut],
};
PythonShell.run('models/model.py', options, function (err, results) {
  if (err) throw err;
  console.log('results: %j', results);
});
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

app.listen(3000)
