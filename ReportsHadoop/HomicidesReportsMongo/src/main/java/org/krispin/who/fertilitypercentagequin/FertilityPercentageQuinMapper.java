package org.krispin.who.fertilitypercentagequin;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FertilityPercentageQuinMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private final static FloatWritable fertilityPercentage = new FloatWritable();
    private Text keyText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(";");

        if (columns.length >= 6) {
            String country = columns[0];
            String year = columns[1];
            String educationLevel = columns[2];
            String residenceArea = columns[3];
            String wealthQuintile = columns[4];
            String fertilityValueStr = columns[5];

            if (country.equals("Country or Area") || year.equals("Year(s)") || fertilityValueStr.equals("Value")) {
                // Ignorar la primera fila de encabezado
                return;
            }

            if (educationLevel.isEmpty() || educationLevel.equals("null") || residenceArea.isEmpty() || residenceArea.equals("null") || wealthQuintile.isEmpty() || wealthQuintile.equals("null") || wealthQuintile.equals(" \n") || wealthQuintile.equals(" ")) {
                // Ignorar las filas sin nivel educativo, área de residencia o quintil de riqueza
                return;
            }

            if (fertilityValueStr.isEmpty() || fertilityValueStr.equals("null")) {
                // Ignorar las filas sin valor de fertilidad
                return;
            }

            try {
                float fertilityValue = Float.parseFloat(fertilityValueStr);
                int decade = Integer.parseInt(year.substring(0, 3) + "0");
                String keyEl = country + "\t" + wealthQuintile + "\t" + decade;

                keyText.set(keyEl);
                fertilityPercentage.set(fertilityValue);
                context.write(keyText, fertilityPercentage);
            } catch (NumberFormatException e) {
                // Ignorar filas con valores de fertilidad no numéricos
            }

        }
    }
}

