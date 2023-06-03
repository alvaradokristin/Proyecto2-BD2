package org.krispin.homicides.vicsxsexo;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VicsXSexoMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private final static FloatWritable homicideCount = new FloatWritable();
    private Text countrySex = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(";");

        if (columns.length >= 10) {
            String sex = columns[3];
            float totalHomicides = 0.0f;
            int numYears = 0;

            if (sex.equals("Gender") || sex.isEmpty() || sex.equals("null")) {
                // Ignorar la primera fila de encabezado
                return;
            }

            for (int i = 4; i < columns.length; i++) {
                if (!columns[i].isEmpty() && !columns[i].equals("null")) {
                    try {
                        float homicides = parseHomicidesValue(columns[i]);
                        totalHomicides += homicides;
                        numYears++;
                    } catch (NumberFormatException e) {
                        // Ignorar los valores no numéricos
                        continue;
                    }
                }
            }

            if (numYears > 0) {
                float averageHomicides = totalHomicides / numYears;
                String keyText = sex;
                countrySex.set(keyText);
                homicideCount.set(averageHomicides);
                context.write(countrySex, homicideCount);
            }
        }
    }

    private float parseHomicidesValue(String value) throws NumberFormatException {
        if (value.contains(".")) {
            return Float.parseFloat(value);
        } else {
            return Integer.parseInt(value);
        }
    }
}