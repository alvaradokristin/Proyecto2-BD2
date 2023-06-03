package org.krispin.homicides.prommaxminxregionxsexodec;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PromMaxMinXRegionXSexoDecMapper extends Mapper<Object, Text, Text, FloatWritable> {
    private final static FloatWritable homicideData = new FloatWritable();
    private Text keyText = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(";");

        if (columns.length >= 10) {
            String region = columns[0];
            String gender = columns[3];

            if (region.equals("Region") || gender.isEmpty() || gender.equals("null")) {
                // Ignorar la primera fila de encabezado y las filas sin sexo
                return;
            }

            if (region.equals("(blank)")) {
                // Ignorar las filas con la región: "(blank)"
                return;
            }

            for (int i = 4; i < columns.length; i++) {
                if (!columns[i].isEmpty() && !columns[i].equals("null")) {
                    if (StringUtils.isNumeric(columns[i])) {
                        float homicides = Float.parseFloat(columns[i].replace(",", "."));
                        int decade = 2000 + (((i - 4) / 21) * 10);
                        String keyEl = region + "\t" + gender + "\t" + decade;

                        keyText.set(keyEl);
                        homicideData.set(homicides);
                        context.write(keyText, homicideData);
                    }
                }
            }
        }
    }
}