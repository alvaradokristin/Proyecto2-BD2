package org.krispin.who.cancarrespxpaisanno;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;

public class CanCarRespXPaisAnnoRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Deaths Calculation");
        job.setJarByClass(CanCarRespXPaisAnnoRunner.class);
        job.setMapperClass(CanCarRespXPaisAnnoMapper.class);
        job.setReducerClass(CanCarRespXPaisAnnoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if (job.waitForCompletion(true)) {
            // Crear archivo "result.txt" con los resultados
            Path outputPath = new Path(args[1]);
            Path resultFilePath = new Path(outputPath, "result.txt");

            try (FileSystem fs = outputPath.getFileSystem(conf);
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(resultFilePath)))) {

                // Leer el archivo de salida del MapReduce y agregar su contenido al archivo "result.txt"
                Path mapReduceOutputPath = new Path(args[1]);  // Ruta del directorio de salida del MapReduce
                Path mapReduceResultFilePath = new Path(mapReduceOutputPath, "part-r-00000");  // Ruta del archivo de salida del MapReduce

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(mapReduceResultFilePath)))) {

                    // Leer línea por línea del archivo de salida del MapReduce
                    String line;
                    while ((line = reader.readLine()) != null) {
                        // Agregar cada línea al archivo "result.txt"
                        writer.write(line);
                        writer.newLine();
                    }

                } catch (IOException e) {
                    System.err.println("Error al leer el archivo de salida del MapReduce: " + e.getMessage());
                }

                writer.flush();
                System.out.println("Archivo 'result.txt' creado exitosamente.");

            } catch (IOException e) {
                System.err.println("Error al crear el archivo 'result.txt': " + e.getMessage());
            }
        } else {
            System.err.println("Error en el trabajo MapReduce. No se creará el archivo 'result.txt'.");
        }
    }
}
