/******************  This route get the data of one medication in one training *******************/

/********* ALL TRAINING => ONE TRAINING BY MEDICATION => ONE DELEDICATION DEATILS *********/

const express = require("express");
const db = require("../../database");
const router = express.Router();

// test
router.get("/", (req, res) => {
  res.json({ msg: "am the Details of one training" });
});

const cassandra = require("cassandra-driver");
const { request } = require("express");

const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "frauddetection",
});

// get Details of one medication in the training  with NumEnR and idEntrainement
router.get("/OneMedication/", (req, res) => {
  const NumEnR = req.query.NumEnR;
  const idEntrainement = req.query.idEntrainement;
  const query =
    "SELECT * FROM Quantity_result WHERE  num_enr= ? AND id_entrainement= ?  ALLOW FILTERING ";
  client
    .execute(query, [NumEnR, idEntrainement], { prepare: true })
    .then((result) => {
      console.log(result);
      var oneHistoryByNum_EnrAndIdEntrainement = result;
      //The row is an Object with column names as property keys.
      res.status(200).send(oneHistoryByNum_EnrAndIdEntrainement?.rows);
    })
    .catch((err) => {
      console.log("ERROR :", err);
    });
});

// find the nomber of assurée suspected in one medication
router.get("/CountAssuresSuspected/", (req, res) => {
  const query =
    "select count_assure , no_assure , num_enr from assure_result where id_entrainement = ? and num_enr = ?  group by no_assure ALLOW FILTERING ;";
  const idEntrainement = req.query.idEntrainement;
  const numEnr = req.query.numEnr;

  client
    .execute(query, [idEntrainement, numEnr], { prepare: true })
    .then((result) => {
      console.log(result);
      var ResultCountPerAssure = result;
      //The row is an Object with column names as property keys.
      res.status(200).send(ResultCountPerAssure?.rows);
    })
    .catch((err) => {
      res.status(400).send("err");
      console.log("ERROR :", err);
    });
});

module.exports = router;
