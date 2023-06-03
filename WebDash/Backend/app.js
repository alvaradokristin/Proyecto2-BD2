const express = require("express");
const { startConnection } = require("./MongoDB/DB/database.js");
const mongoose = require('mongoose');
const app = express();
const homicidesRoutes = require('./MongoDB/routes/homicides.routes.js');


app.use(express.static("public"));

app.set("view engine", "ejs");
app.set("views", __dirname + "/../Frontend/views");

const path = require("path");
// set the correct route for public folder
app.use(express.static(path.join(__dirname, "..", "Frontend", "public")));

app.get("/", function (req, res) {
  res.render("index");
});

app.listen(3000, () => {
  console.log("La aplicación está escuchando en el puerto 3000.");
});

async function main() {
  await startConnection();
}



app.use ('/api',homicidesRoutes);

main();
