package org.krispin.who.populationbydecade;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

public class PopulationByDecadeReducer extends Reducer<Text, IntWritable, Text, Text> {
    private Text result = new Text();
    private List<String> outputList = new ArrayList<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable val : values) {
            int current = val.get();
            sum += current;
        }

        String output = key.toString() + "\t" + sum;
        result.set(output);
        context.write(key, result);

        // Agrega los resultados a la lista
        outputList.add(output);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        // Guarda los resultados en el archivo "result.txt"
        try {
            Path outputPath = FileOutputFormat.getOutputPath(context);
            Path resultFilePath = new Path(outputPath, "result.txt");
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputPath.getFileSystem(context.getConfiguration()).create(resultFilePath, true)));

            for (String resultLine : outputList) {
                writer.write(resultLine);
                writer.newLine();
            }

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}