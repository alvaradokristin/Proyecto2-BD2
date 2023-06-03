package org.krispin.who.prommaxmincomun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class PromMaxMinComunRunner {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "HomicidesReport");
        job.setJarByClass(PromMaxMinComunRunner.class);
        job.setMapperClass(PromMaxMinComunMapper.class);
        job.setReducerClass(PromMaxMinComunReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        // Crear archivo "result.txt" con los resultados
        Path outputPath = new Path(args[1]);
        Path resultFilePath = new Path(outputPath, "result.txt");

        try (FileSystem fs = outputPath.getFileSystem(conf);
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(resultFilePath)))) {

            // Agregar los resultados al archivo "result.txt"
            writer.write("Resultados del MapReduce:\n");
            // Aquí puedes agregar cualquier información adicional que desees en el archivo

            // Ejemplo: Agregar los argumentos pasados al programa
            writer.write("Argumentos del programa:\n");
            for (String arg : args) {
                writer.write(arg);
                writer.write("\n");
            }

            // También puedes agregar los resultados específicos del MapReduce
            writer.write("Resultados específicos:\n");
            // Agrega aquí los resultados que deseas guardar en el archivo

            writer.flush();
            System.out.println("Archivo 'result.txt' creado exitosamente.");
        } catch (IOException e) {
            System.err.println("Error al crear el archivo 'result.txt': " + e.getMessage());
        }
    }
}
