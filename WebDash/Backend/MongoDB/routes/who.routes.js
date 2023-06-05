const express = require("express");
const { getWhoReport } = require("../controllers/who.controllers.js");

const router = express.Router();
router.get('/getWhoReport/:reportType', getWhoReport);

module.exports = router;
