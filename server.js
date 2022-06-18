const express = require('express')
const session = require("express-session");
const cors = require("cors");
const cookieParser = require("cookie-parser");
const bodyParser = require("body-parser");

const app = express()
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
// les routes 
const authRouter = require('./routes/auth')
const modelsRoute = require('./routes/models')


app.use('/models', modelsRoute);




app.use(session({
  secret  : 'secret_code',
  resave: true,
  saveUninitialized: true,
}))



const query = 'SELECT * FROM user';
 
/*client.execute(query)
  .then(result => console.log('User with username %s', result.rows[1].username));*/

/***************************************************************************************************************** */


//routes
app.get("/",(req,res)=>{
  //this is just a test route
  res.json({toto:"ioi"})
})




app.listen(3000)
