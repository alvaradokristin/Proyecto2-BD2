package org.krispin.homicides.prommaxminxregion;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PromMaxMinXRegionMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private final static FloatWritable homicideData = new FloatWritable();
    private Text keyText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(";");

        if (columns.length >= 10) {
            String region = columns[0];

            if (region.equals("Region")) {
                // Ignorar la primera fila de encabezado
                return;
            }

            for (int i = 4; i < columns.length; i++) {
                if (!columns[i].isEmpty() && !columns[i].equals("null")) {
                    float homicides = Float.parseFloat(columns[i].replace(",", "."));
                    int year = 2000 + ((i - 4) % 21);
                    String keyEl = region + "\t" + year;

                    keyText.set(keyEl);
                    homicideData.set(homicides);
                    context.write(keyText, homicideData);
                }
            }
        }
    }
}