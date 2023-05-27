package org.krispin.homicides.promvicxpais;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PromVicXPaisMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
    private final static FloatWritable homicideCount = new FloatWritable();
    private Text country = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(";");

        if (columns.length >= 30) {
            String countryName = columns[2];
            float totalHomicides = 0.0f;
            int numYears = 0;

            if (countryName.equals("country")) {
                // Ignorar la primera fila de encabezado
                return;
            }

            if (countryName.isEmpty() || countryName.equals("null")) {
                // Ignorar las filas sin nombre de país
                return;
            }

            if (columns[3].equals("Homicidios")) {
                // Ignorar las filas de encabezado adicionales
                return;
            }

            if (columns[3].isEmpty() || columns[3].equals("null")) {
                // Ignorar las filas sin datos de homicidio
                return;
            }

            for (int i = 4; i < columns.length; i++) {
                if (!columns[i].isEmpty() && !columns[i].equals("null")) {
                    totalHomicides += Float.parseFloat(columns[i].replace(",", "."));
                    numYears++;
                }
            }

            if (numYears > 0) {
                float averageHomicides = totalHomicides / numYears;
                country.set(countryName);
                homicideCount.set(averageHomicides);
                context.write(country, homicideCount);
            }
        }
    }
}