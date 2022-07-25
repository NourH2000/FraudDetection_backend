const pythonShellScript = require("../../helpers").pythonShellScript;

const { json } = require("body-parser");
const express = require("express");
const db = require("../../database");
const router = express.Router();

//data base connection
const cassandra = require("cassandra-driver");
const { request } = require("express");
const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "cnas",
});

// test route
router.get("/", (req, res) => {
  res.json({ toto: "models" });
});

//get the maximum date

router.get("/getMaxdate", (req, res) => {
  const query = "SELECT MAX(date_paiement) as date_paiement_Max FROM cnas";
  try {
    client.execute(query, function (err, result) {
      var maxDate = result?.rows[0];
      //The row is an Object with column names as property keys.
      res.status(200).send(maxDate);
    });
  } catch (err) {
    console.log(err);
  }
});

//call a model ( quantitymodel)
router.post("/quantitymodel", (req, res) => {
  const date_debut = req.body.date_debut;
  const date_fin = req.body.date_fin;

  var options = {
    //scriptPath: '',
    //replace this dates with the ones you will receive from req.body
    args: [date_debut, date_fin],
  };
  const path = "IAModels/QuantityModel.py";
  try {
    pythonShellScript(path, options);
    console.log("hello am a quantity model ");
  } catch (err) {
    res.send(err);
  }
});

//call a model ( PPaModel)
router.post("/ppamodel", (req, res) => {
  const date_debut = req.body.date_debut;
  const date_fin = req.body.date_fin;

  var options = {
    //scriptPath: '',
    //replace this dates with the ones you will receive from req.body
    args: [date_debut, date_fin],
  };
  const path = "IAModels/PrixppaModel.py";
  try {
    pythonShellScript(path, options);
    console.log("hello am a ppa model ");
  } catch (err) {
    res.send(err);
  }
});

//get the minimum date

router.get("/getMindate", (req, res) => {
  const query = "SELECT MIN(date_paiement) as date_paiement_Min FROM cnas ";
  try {
    client.execute(query, function (err, result) {
      var minDate = result?.rows[0];
      //The row is an Object with column names as property keys.
      res.status(200).send(minDate);
    });
  } catch (err) {
    console.log(err);
  }
});

///
const clientTwo = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "frauddetection",
});

// get if the this training exists before
router.get("/TestTraining", (req, res) => {
  const query =
    "SELECT count(*) FROM History where type = ? and date_debut = ? and date_fin = ? and status = ? ALLOW FILTERING  ;";
  const type = req.query.type;
  const date_debut = req.query.date_debut;
  const date_fin = req.query.date_fin;
  const status = 1;
  clientTwo
    .execute(query, [type, date_debut, date_fin, status], { prepare: true })
    .then((result) => {
      var TestDate = result;
      res.status(200).send(TestDate?.rows);
    })
    .catch((err) => {
      console.log("ERROR :", err);
    });
});

module.exports = router;
