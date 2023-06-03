package org.krispin.who.populationbydecade;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PopulationByDecadeMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable population = new IntWritable();
    private Text keyText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(";");

        if (columns.length >= 3) {
            String country = columns[0];
            String year = columns[1];
            String populationStr = columns[2];

            if (country.equals("Country") || year.isEmpty() || year.equals("null") || populationStr.isEmpty() || populationStr.equals("null")) {
                // Ignorar la primera fila de encabezado y las filas sin año o población
                return;
            }

            if (country.equals("(blank)")) {
                // Ignorar las filas con el país: "(blank)"
                return;
            }

            if (year.length() >= 3) {
                int decade = Integer.parseInt(year.substring(0, 3) + "0");
                int populationValue = Integer.parseInt(populationStr.replace(",", ""));

                String keyEl = country + "\t" + decade;

                keyText.set(keyEl);
                population.set(populationValue);
                context.write(keyText, population);
            }
        }
    }
}