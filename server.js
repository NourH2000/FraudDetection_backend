const express = require('express')
const session = require("express-session");
const cors = require("cors");
const cookieParser = require("cookie-parser");
const bodyParser = require("body-parser");
const passport = require("passport");
const local = require("./strategies/local");

const app = express()
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

//session
app.use(session({
  secret  : 'secret_code',
  cookie : { maxAge: 3000},
  resave: true,
  saveUninitialized: false,
}))


// les routes 
const authRouter = require('./routes/auth')
const modelsRoute = require('./routes/models')
const historiqueRoute = require('./routes/historique')
const usersRoute = require('./routes/users')


app.use('/models', modelsRoute);
app.use('/historique',historiqueRoute);
app.use('/users', usersRoute);
app.use('/auth', authRouter); 

app.use(passport.initialize());
app.use(passport.session());








 
/*client.execute(query)
  .then(result => console.log('User with username %s', result.rows[1].username));*/

/***************************************************************************************************************** */


//routes
app.get("/",(req,res)=>{
  //this is just a test route
  res.json({toto:"ioi"})
})




app.listen(8000)
