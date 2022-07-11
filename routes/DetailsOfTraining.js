const express = require("express");
const db = require("../database");
const router = express.Router();

// test request
router.get("/", (req, res) => {
  res.json({ toto: "Details Of Training Route" });
});
const cassandra = require("cassandra-driver");
const { request } = require("express");

const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "fraud",
});

// find the nomber of drugs suspected
router.get("/CountMedicamentSuspected/", (req, res) => {
  const query =
    "select count(*) , num_enr from Quantity_result where id_entrainement = ? group by num_enr ALLOW FILTERING ;";
  const idEntrainement = req.query.idEntrainement;

  client
    .execute(query, [idEntrainement], { prepare: true })
    .then((result) => {
      console.log(result);
      var ResultCountPerNumEnr = result;
      //The row is an Object with column names as property keys.
      res.status(200).send(ResultCountPerNumEnr?.rows);
    })
    .catch((err) => {
      console.log("ERROR :", err);
    });
});

module.exports = router;
