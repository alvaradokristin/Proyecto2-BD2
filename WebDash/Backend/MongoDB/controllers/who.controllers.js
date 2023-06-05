
const Who = require('../models/who.model.js');

async function getWhoReport(req, res) {
    try {
      
      const WhoReport = await  Who.find({report: req.params.reportType});
      const filterData = WhoReport[0]
      
      if (WhoReport.length === 0) {
        return res.status(404).json({ error: 'Reporte no encontrado' });
      }
      return res.json(filterData);
      
    } catch (error) {
      return res.status(500).json({ error: 'Error en el servidor' });
    }

  }

  module.exports = {
    getWhoReport
  };