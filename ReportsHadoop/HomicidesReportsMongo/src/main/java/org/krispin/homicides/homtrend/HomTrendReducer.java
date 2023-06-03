package org.krispin.homicides.homtrend;

import org.apache.hadoop.fs.FileSystem;
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

public class HomTrendReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable totalHomicides = new IntWritable();
    private List<String> outputList = new ArrayList<>();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        totalHomicides.set(sum);
        context.write(key, totalHomicides);

        // Agrega los resultados a la lista
        String resultLine = key.toString() + "\t" + totalHomicides;
        outputList.add(resultLine);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);

        // Guarda los resultados en el archivo "result.txt"
        try {
            Path outputPath = FileOutputFormat.getOutputPath(context);
            Path resultFilePath = new Path(outputPath, "result.txt");
            FileSystem fs = outputPath.getFileSystem(context.getConfiguration());

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(resultFilePath, true)));

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