package org.krispin.homicides.toptvicsxpais;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopTVicsXPaisMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text year = new Text();
    private Text countryCount = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");

        if (parts.length >= 2) {
            String yearCountry = parts[0];
            String country = yearCountry.split(",")[1];
            String count = parts[1];

            year.set(yearCountry.split(",")[0]);
            countryCount.set(country + "," + count);
            context.write(year, countryCount);
        } else {
            // Manejar el caso en el que la línea no tenga suficientes elementos
            // o no cumpla con el formato esperado.
            System.out.println("Err");
        }
    }
}
