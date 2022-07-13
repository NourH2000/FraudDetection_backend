const express = require("express");
const session = require("express-session");
const cors = require("cors");
const cookieParser = require("cookie-parser");
const bodyParser = require("body-parser");
const passport = require("passport");
const local = require("./strategies/local");

const app = express();
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));
app.use(cors());

//session
app.use(
  session({
    secret: "secret_code",
    cookie: { maxAge: 3000 },
    resave: true,
    saveUninitialized: false,
  })
);

// les routes
const authRouter = require("./routes/Authentification/auth");
const modelsRoute = require("./routes/Models/models");
const historiqueQRoute = require("./routes/Quantity/historique");
const usersRoute = require("./routes/Authentification/users");
const DetailsTrainingQRoute = require("./routes/Quantity/OneTraining");
const DetailsOfMedicationQRoute = require("./routes/Quantity/OneMedication");

app.use("/models", modelsRoute);
app.use("/historiqueQ", historiqueQRoute);
app.use("/users", usersRoute);
app.use("/auth", authRouter);
app.use("/DetailsOfTrainingQ", DetailsTrainingQRoute);
app.use("/DetailsOfMedicationQ", DetailsOfMedicationQRoute);

app.use(passport.initialize());
app.use(passport.session());

/*client.execute(query)
  .then(result => console.log('User with username %s', result.rows[1].username));*/

/***************************************************************************************************************** */

//routes
app.get("/", (req, res) => {
  //this is just a test route
  res.json({ toto: "ioi" });
});

app.listen(8000);
