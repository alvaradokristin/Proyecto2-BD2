const mongoose = require('mongoose');

const homicidesSchema = mongoose.Schema({
    _id:String,
    report:String,
    country:Array,
    region:Array,
    subregion:Array,
    sex:Array,
    total:Array,
    average:Array,
    max:Array,
    min:Array,
    year:Array,
    decade:Array
});

module.exports = mongoose.model('homicides', homicidesSchema);
