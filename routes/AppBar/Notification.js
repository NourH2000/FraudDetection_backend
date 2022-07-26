const express = require("express");
const db = require("../../database");
const router = express.Router();

const cassandra = require("cassandra-driver");
const { request } = require("express");

const client = new cassandra.Client({
  contactPoints: ["127.0.0.1"],
  localDataCenter: "datacenter1",
  keyspace: "frauddetection",
});

// test request
router.get("/", (req, res) => {
  res.json({ toto: "historiqueRoute" });
});

// get count of notifications unseen yet
router.get("/NotificationCount", (req, res) => {
  const query = "SELECT  id FROM History where seen = ?   ALLOW FILTERING   ;";
  const seen = 0;

  client
    .execute(query, [seen], { prepare: true })
    .then((result) => {
      var historyOfTraining = result;
      res.status(200).send(historyOfTraining?.rows);
    })
    .catch((err) => {
      console.log("ERROR :", err);
    });
});

// get data of notifications unseen yet
router.get("/NotificationData", (req, res) => {
  const query = "SELECT * FROM History where seen = ?  ALLOW FILTERING   ;";
  const seen = 0;

  client
    .execute(query, [seen], { prepare: true })
    .then((result) => {
      var historyOfTraining = result;
      res.status(200).send(historyOfTraining?.rows);
    })
    .catch((err) => {
      console.log("ERROR :", err);
    });
});

// get data of notifications unseen yet
router.get("/NotificationUpdate", (req, res) => {
  const query = "UPDATE History SET seen =? where type in(1,2) and id=?   ;";
  const seen = 1;
  const ids = req.query.ids;
  client
    .execute(query, [seen, ids], { prepare: true })
    .then((result) => {
      var historyOfTraining = result;
      res.status(200).send(historyOfTraining?.rows);
    })
    .catch((err) => {
      console.log("ER0OR :", err);
    });
});

module.exports = router;
