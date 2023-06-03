package org.krispin.who.cancarrespxpaisanno;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CanCarRespXPaisAnnoMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(";");

        if (columns.length >= 9) {
            String country = columns[0];
            String year = columns[1];

            if (!columns[8].equals("All")) { // Verificar que el valor no sea "All"
                float totalDeaths = Float.parseFloat(columns[8]);
                float cancerDeaths = Float.parseFloat(columns[2]);
                float respiratoryDeaths = Float.parseFloat(columns[3]);
                float cardiovascularDeaths = Float.parseFloat(columns[4]);

                float cancerPercentage = (cancerDeaths / totalDeaths) * 100;
                float respiratoryPercentage = (respiratoryDeaths / totalDeaths) * 100;
                float cardiovascularPercentage = (cardiovascularDeaths / totalDeaths) * 100;

                String output = String.format("%.2f", cancerPercentage) + "\t" + String.format("%.2f", cardiovascularPercentage) + "\t" + String.format("%.2f", respiratoryPercentage);

                outputKey.set(country + "\t" + year);
                outputValue.set(output);

                context.write(outputKey, outputValue);
            }
        }
    }
}
