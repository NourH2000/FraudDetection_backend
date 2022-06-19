const LocalStrategy = require('passport-local');
const passport = require('passport');
const db = require('../database');

passport.use(new LocalStrategy(
    async ( username , password , done ) => {
        const query = 'SELECT * FROM user';           
        client.execute(query, function (err, result) {
            var histoty = result
            //The row is an Object with column names as property keys. 
            res.status(200).send(histoty?.rows); 

          });     

    }
));