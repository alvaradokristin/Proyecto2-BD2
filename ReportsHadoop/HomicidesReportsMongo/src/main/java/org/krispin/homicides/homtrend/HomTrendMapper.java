package org.krispin.homicides.homtrend;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HomTrendMapper extends Mapper<Object, Text, Text, IntWritable> {
    private Text country = new Text();
    private IntWritable homicides = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(";");

        String countryName = parts[2];

        if (countryName.equals("country")) {
            // Ignorar la primera fila de encabezado
            return;
        }

        for (int i = 4; i < parts.length; i++) {
            if (!parts[i].isEmpty()) {
                int year = 2000 + i - 4;
                int homicideCount = Integer.parseInt(parts[i]);
                country.set(countryName);
                homicides.set(homicideCount);
                context.write(country, homicides);
            }
        }
    }
}