package org.krispin.homicides.promvicxpais;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

public class PromVicXPaisRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "HomicidesReport");
        job.setJarByClass(PromVicXPaisRunner.class);
        job.setMapperClass(PromVicXPaisMapper.class);
        job.setCombinerClass(PromVicXPaisReducer.class);
        job.setReducerClass(PromVicXPaisReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);

        if (success) {
            // Obtener la ruta de salida del job de Hadoop
            Path outputDir = FileOutputFormat.getOutputPath(job);

            // Leer los archivos de salida generados por el job
            FileSystem fs = outputDir.getFileSystem(conf);
            FileStatus[] fileStatuses = fs.listStatus(outputDir);
            List<String> paises = new ArrayList<>();
            List<Float> resultados = new ArrayList<>();

            for (FileStatus status : fileStatuses) {
                if (status.isFile() && status.getPath().getName().startsWith("part")) {
                    // Leer el contenido de cada archivo de salida
                    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            String[] parts = line.split("\t");
                            String pais = parts[0];
                            float resultado = Float.parseFloat(parts[1]);
                            paises.add(pais);
                            resultados.add(resultado);
                        }
                    }
                }
            }

            // =========================== MONGO SET UP=========================== \\
//            String connString = "mongodb+srv://josuemartinezcr:bd21234@bd2-p2.d37xqum.mongodb.net/?retryWrites=true&w=majority";
//            MongoClient client = MongoClients.create(connString);
//
//            MongoDatabase database = client.getDatabase("test");
//            MongoCollection<Document> collection = database.getCollection("exampleReport");
//
//            // Crear el nuevo documento
//            Document newDoc = new Document();
//            newDoc.append("pais", paises);
//            newDoc.append("valor", resultados);
//
//            // Insertar el nuevo documento en la colección de MongoDB
//            collection.insertOne(newDoc);
        }
    }
}
