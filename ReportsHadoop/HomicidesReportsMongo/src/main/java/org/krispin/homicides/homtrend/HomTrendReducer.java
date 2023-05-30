package org.krispin.homicides.homtrend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HomTrendReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable totalHomicides = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value : values) {
            sum += value.get();
        }

        totalHomicides.set(sum);
        context.write(key, totalHomicides);
    }
}