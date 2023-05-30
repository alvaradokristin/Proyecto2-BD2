package org.krispin.homicides.toptvicsxpais;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.Comparator;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopTVicsXPaisReducer extends Reducer<Text, Text, Text, Text> {
    private TreeMap<Float, String> top3Countries = new TreeMap<>(Comparator.reverseOrder());

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        top3Countries.clear();

        for (Text value : values) {
            String[] parts = value.toString().split(",");
            String country = parts[0];
            float count = Float.parseFloat(parts[1]);
            top3Countries.put(count, country);

            if (top3Countries.size() > 3) {
                top3Countries.remove(top3Countries.lastKey());
            }
        }

        for (Map.Entry<Float, String> entry : top3Countries.entrySet()) {
            context.write(key, new Text(entry.getValue() + "," + entry.getKey()));
        }
    }
}