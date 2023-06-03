package org.krispin.who.cancarrespxpaisanno;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CanCarRespXPaisAnnoReducer extends Reducer<Text, FloatWritable, Text, Text> {
    private Text result = new Text();
    private List<String> outputList = new ArrayList<>();

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, value);

            // Agrega los resultados a la lista
            String resultLine = key + "\t" + value;
            outputList.add(resultLine);
        }
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
