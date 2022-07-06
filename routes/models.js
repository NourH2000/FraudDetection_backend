const pythonShellScript = require("../helpers").pythonShellScript;

const { json } = require("body-parser");
const express = require("express");
const db = require("../database");
const router = express.Router();

//data base connection
const cassandra = require("cassandra-driver");
const { request } = require("express");
const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "fraud",
});

// test route
router.get("/", (req, res) => {
  res.json({ toto: "models" });
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
  const path = "models/QuantityModel.py";
  try {
    pythonShellScript(path, options);
    console.log("hello");
  } catch (err) {
    res.send(err);
  }
});

//get the maximum date

router.get("/getMaxdate", (req, res) => {
  const query =
    "SELECT MAX(date_paiement) as date_paiement_Max FROM Quantity_result";
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

//get the minimum date

router.get("/getMindate", (req, res) => {
  const query =
    "SELECT MIN(date_paiement) as date_paiement_Min FROM Quantity_result ";
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

module.exports = router;
