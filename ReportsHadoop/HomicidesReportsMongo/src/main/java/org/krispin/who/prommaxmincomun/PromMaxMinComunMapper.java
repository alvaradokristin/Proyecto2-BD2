package org.krispin.who.prommaxmincomun;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PromMaxMinComunMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private final static FloatWritable deathsData = new FloatWritable();
    private Text keyText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(";");

        if (columns.length >= 1) {
            String country = columns[0];

            if (country.equals("Country")) {
                // Ignorar la primera fila de encabezado
                return;
            }

            float deaths = Float.parseFloat(columns[2]);
            String keyEl = country;

            keyText.set(keyEl);
            deathsData.set(deaths);
            context.write(keyText, deathsData);
        }
    }
}