const mongoose = require('mongoose');
const startConnection = async function() {
    try{
        await mongoose.connect('mongodb+srv://josuemartinezcr:bd21234@bd2-p2.d37xqum.mongodb.net/projectReports?retryWrites=true&w=majority');
        console.log("DATABASE CONNECTED"); 
    } catch(error){
        console.log("Conexión fallida: ", error)
    }
}
module.exports = { startConnection };