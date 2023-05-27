package org.krispin.homicides.promvicxpais;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class PromVicXPaisReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        float sum = 0.0f;
        int count = 0;

        for (FloatWritable val : values) {
            sum += val.get();
            count++;
        }

        float average = sum / count;
        result.set(average);
        context.write(key, result);

        // =========================== MONGO SET UP=========================== \\

//        String connString = "mongodb+srv://josuemartinezcr:bd21234@bd2-p2.d37xqum.mongodb.net/?retryWrites=true&w=majority";
//        MongoClient client = MongoClients.create(connString);
//
//        MongoDatabase database = client.getDatabase("test");
//        MongoCollection<Document> collection = database.getCollection("exampleReport");
//
//        // Crear el nuevo documento
//        Document newDoc = new Document();
//        newDoc.append("pais", key.toString());
//        newDoc.append("valor", result.get());
//
//        // Insertar el nuevo documento en la colección de MongoDB
//        collection.insertOne(newDoc);
    }
}