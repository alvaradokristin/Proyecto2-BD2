const express = require("express");
const { getHomicidesReport } = require("../controllers/homicides.controllers.js");

const router = express.Router();
router.get('/getHomicideReport/:reportType', getHomicidesReport);

module.exports = router;
