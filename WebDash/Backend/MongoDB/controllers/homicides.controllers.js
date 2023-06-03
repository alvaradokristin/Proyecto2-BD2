
const homicides = require('../models/homicides.model.js');

async function getHomicidesReport(req, res) {
    try {

      const homicideReport = await  homicides.find({report: req.params.reportType});
      const filterData = homicideReport[0]

      if (!homicideReport) {
        return res.status(404).json({ error: 'Reporte no encontrado' });
      }
      
      return res.json(filterData);
      
    
    } catch (error) {
      return res.status(500).json({ error: 'Error en el servidor' });
    }
  }

  module.exports = {
    getHomicidesReport
  };