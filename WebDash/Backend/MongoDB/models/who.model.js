const mongoose = require('mongoose');

const whoSchema = mongoose.Schema(
  {
    _id: String,
    report: String,
    country: Array,
    year: Array,
    decade: Array,
    quintil: Array,
    population: Array,
    percentage: Array,
    cancer: Array,
    cardiovascular: Array,
    respiratory: Array,
  },
  { collection: 'who' } // Especifica el nombre de la colección como 'who'
);

module.exports = mongoose.model('who', whoSchema);