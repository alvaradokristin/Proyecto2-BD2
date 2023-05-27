package org.krispin.homicides.prommaxminxregion;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PromMaxMinXRegionReducer extends Reducer<Text, FloatWritable, Text, Text> {
    private Text result = new Text();

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

        String output = "Promedio: " + average + "\tMaximo: " + max + "\tMinimo: " + min;
        result.set(output);
        context.write(key, result);
    }
}
