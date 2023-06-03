package org.krispin.homicides.prommaxminxsubregionxsexodec;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

public class PromMaxMinXSubregionXSexoDecReducer extends Reducer<Text, FloatWritable, Text, Text> {
    private Text result = new Text();
    private List<String> outputList = new ArrayList<>();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float sum = 0.0f;
        int count = 0;
        float max = Float.MIN_VALUE;
        float min = Float.MAX_VALUE;

        for (FloatWritable val : values) {
            float current = val.get();
            sum += current;
            count++;
            if (current > max) {
                max = current;
            }
            if (current < min) {
                min = current;
            }
        }

        float average = sum / count;

        String output = average + "\t" + max + "\t" + min;
        result.set(output);
        context.write(key, result);

        // Agrega los resultados a la lista
        String resultLine = key.toString() + "\t" + output;
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