/*************************  This route get the the data of one training Grouped By Medication *************************/

/********* ALL TRAINING => ONE TRAINING BY MEDICATION  *********/

const express = require("express");
const db = require("../../database");
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
  keyspace: "frauddetection",
});

// get all the History Of ppa_result groupedBy num_enr and idEntrainement
router.get("/ByMedication/", (req, res) => {
  const query =
    "select * from ppa_result where id_entrainement = ? group by num_enr ALLOW FILTERING ;";
  const idEntrainement = req.query.idEntrainement;

  client
    .execute(query, [idEntrainement], { prepare: true })
    .then((result) => {
      //console.log(result);
      var ResultGroupedByNumEnrAndID = result;
      //The row is an Object with column names as property keys.
      res.status(200).send(ResultGroupedByNumEnrAndID?.rows);
    })
    .catch((err) => {
      console.log("ERROR :", err);
    });
});

// find the nomber of drugs suspected
router.get("/CountMedicamentSuspected/", (req, res) => {
  const query =
    "select count_medicament_suspected as count , num_enr from ppa_result where id_entrainement = ? group by num_enr ALLOW FILTERING ;";
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

// get count of each centre By medication ( center , medication )
router.get("/CountCenterSuspected/", (req, res) => {
  const query =
    "select count(*), num_enr from ppa_result where id_entrainement =? and centre =?  group by num_enr ALLOW FILTERING ;";

  const idEntrainement = req.query.idEntrainement;
  const centre = req.query.centre;
  client
    .execute(query, [idEntrainement, centre], { prepare: true })
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

// get count of one centre ( centre => count )
router.get("/CountCenterSuspected/", (req, res) => {
  const query =
    "select count(*) as count  from ppa_result where id_entrainement =? and centre =?   ALLOW FILTERING ;";

  const idEntrainement = req.query.idEntrainement;

  const centre = req.query.centre;
  client
    .execute(query, [idEntrainement, centre], { prepare: true })
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

// get count of each centre ( centre => num_enr ) where grouoedByMedicaùent
router.get("/CountOneCenterMedication/", (req, res) => {
  const query =
    "select count(*) , num_enr    from ppa_result where id_entrainement =? and centre = ? group by num_enr      ALLOW FILTERING ;";

  const idEntrainement = req.query.idEntrainement;
  const centre = req.query.centre;

  client
    .execute(query, [idEntrainement, centre], { prepare: true })
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
// count the number of medication with center : this query will be traited in the front end , because we
// don't have a table that allows us to do a group by Center query

// get count of each centre ( centre => num_enr )
router.get("/CountCenterMedication/", (req, res) => {
  const query =
    "select num_enr , centre from ppa_result where id_entrainement =?    ALLOW FILTERING ;";

  const idEntrainement = req.query.idEntrainement;

  client
    .execute(query, [idEntrainement], { prepare: true })
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

// count the number of medication with Codeps ( pharmacie) : this query will be traited in the front end , because we
// don't have a table that allows us to do a group by codeps query

// get count of each codeps ( codeps => num_enr )
router.get("/CountCodepsMedication/", (req, res) => {
  const query =
    "select num_enr , codeps from ppa_result where id_entrainement =?    ALLOW FILTERING ;";

  const idEntrainement = req.query.idEntrainement;

  client
    .execute(query, [idEntrainement], { prepare: true })
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
