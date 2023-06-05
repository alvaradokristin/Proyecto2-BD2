const express = require("express");
const { startConnection } = require("./MongoDB/DB/database.js");
const mongoose = require('mongoose');
const morgan = require('morgan');
const app = express();
const homicidesRoutes = require('./MongoDB/routes/homicides.routes.js');
const whoRoutes = require('./MongoDB/routes/who.routes.js');
const axios = require('axios');

app.use(express.static("public"));
app.use(morgan('common'));

app.set("view engine", "ejs");
app.set("views", __dirname + "/../Frontend/views");

const path = require("path");
// set the correct route for public folder
app.use(express.static(path.join(__dirname, "..", "Frontend", "public")));

app.get("/", async function (req, res) {
  res.render("index", {type : "none", lista : null});
});

app.get("/showWho/:Report", async function (req, res) {
  const type =  "getWhoReport/";
  const report = req.params.Report;
  console.log(type + report);
  const response = await getData(type + report);
  const data = response.data; // Access the response data
  //console.log(data);
  res.render("index", {type : report, lista : data});
});

app.get("/showHom/:Report", async function (req, res) {
  const type =  "getHomicideReport/";
  const report = req.params.Report;
  const response = await getData(type + report);
  const data = response.data; // Access the response data
  // console.log(data);
  res.render("index", {type : report, lista : data});
});

app.listen(3000, () => {
  console.log("La aplicación está escuchando en el puerto 3000.");
});

async function main() {
  await startConnection();
}

app.use ('/api',whoRoutes,homicidesRoutes);

async function getData(url) {
  console.log(url);
  try {
    const response = await axios.get('http://localhost:3000/api/'+ url);
    return response;
  } catch (error) {
    console.error(error);
  }
}

main();
