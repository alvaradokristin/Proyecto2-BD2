package org.krispin.hadooptomongo;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.bson.Document;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import java.net.URI;
import java.util.Scanner;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class MapReduceResultToMongo {
    private static void insertDataIntoMongoDB(String collections, String report, List<String> countries, List<Integer> total, List<String> region,
                                              List<String> subregion, List<Float> average, List<Float> max,
                                              List<Float> min, List<String> sex, List<Integer> year, List<Integer> decade) {
        String connString = "mongodb+srv://josuemartinezcr:bd21234@bd2-p2.d37xqum.mongodb.net/?retryWrites=true&w=majority";
        MongoClient client = MongoClients.create(connString);
        MongoDatabase database = client.getDatabase("projectReports");
        MongoCollection<Document> collection = database.getCollection(collections);

        Document document = new Document();
        document.append("report", report);
        document.append("country", countries);
        document.append("region", region);
        document.append("subregion", subregion);
        document.append("sex", sex);
        document.append("total", total);
        document.append("average", average);
        document.append("max", max);
        document.append("min", min);
        document.append("year", year);
        document.append("decade", decade);

        collection.insertOne(document);
        client.close();
    }

    private static void readLinesFromHDFS(Path path) throws URISyntaxException {

        String reportTitle = path.getParent().getName();
        String collection = path.getParent().getParent().getName();

        List<String> countries = new ArrayList<>();
        List<Integer> total = new ArrayList<>();
        List<String> regions = new ArrayList<>();
        List<String> subregions = new ArrayList<>();
        List<Float> average = new ArrayList<>();
        List<Float> max = new ArrayList<>();
        List<Float> min = new ArrayList<>();
        List<String> sex = new ArrayList<>();
        List<Integer> years = new ArrayList<>();
        List<Integer> decades = new ArrayList<>();

        try (final DistributedFileSystem dFS = new DistributedFileSystem() {
            {
                initialize(new URI("hdfs://localhost:9000"), new Configuration());
            }
        };
             final FSDataInputStream streamReader = dFS.open(path);
             final Scanner scanner = new Scanner(streamReader);) {

            String line;
            while (scanner.hasNextLine()) {
                line = scanner.nextLine();
                String[] values = line.split("\t");

                switch (reportTitle) {
                    case "PromMaxMinXRegion":
                        regions.add(values[0]);
                        years.add(Integer.parseInt(values[1]));
                        average.add(Float.parseFloat(values[2]));
                        max.add(Float.parseFloat(values[3]));
                        min.add(Float.parseFloat(values[4]));
                        break;
                    case "PromedioVictimas":
                        countries.add(values[0]);
                        average.add(Float.parseFloat(values[1]));
                        break;
                    case "HomicidesTrend":
                        countries.add(values[0]);
                        total.add(Integer.parseInt(values[1]));
                        break;
                    case "PromMaxMinXRegionXSexo":
                        regions.add(values[0]);
                        sex.add(values[1]);
                        years.add(Integer.parseInt(values[2]));
                        average.add(Float.parseFloat(values[3]));
                        max.add(Float.parseFloat(values[4]));
                        min.add(Float.parseFloat(values[5]));
                        break;
                    case "PromVicXPaisXSexo":
                        countries.add(values[0]);
                        sex.add(values[1]);
                        average.add(Float.parseFloat(values[2]));
                        break;
                    case "PromMaxMinXSubregion":
                        subregions.add(values[0]);
                        years.add(Integer.parseInt(values[1]));
                        average.add(Float.parseFloat(values[2]));
                        max.add(Float.parseFloat(values[3]));
                        min.add(Float.parseFloat(values[4]));
                        break;
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        insertDataIntoMongoDB(collection, reportTitle, countries, total, regions, subregions, average, max, min, sex, years, decades);
    }

    public static void main(String[] args) throws URISyntaxException {
        List<Path> paths = new ArrayList<>();

        // ----------------------- HOMICIDIOS ----------------------- \\
        paths.add(new Path("/output/homicides/PromMaxMinXRegion/result.txt"));
        paths.add(new Path("/output/homicides/PromedioVictimas/result.txt"));
        paths.add(new Path("/output/homicides/HomicidesTrend/result.txt"));
        paths.add(new Path("/output/homicides/PromMaxMinXSubregion/result.txt"));
        paths.add(new Path("/output/homicides/PromVicXPaisXSexo/result.txt"));
        paths.add(new Path("/output/homicides/PromMaxMinXRegionXSexo/result.txt"));
        paths.add(new Path("/output/homicides/PromMaxMinXSubregionXSexo/result.txt"));
        paths.add(new Path("/output/homicides/PromMaxMinXRegionXSexoDec/result.txt"));
        paths.add(new Path("/output/homicides/PromMaxMinXSubregionXSexoDec/result.txt"));
        paths.add(new Path("/output/homicides/PromVicsXSexo/result.txt"));

        // ----------------------- WHO ----------------------- \\


        System.setProperty("HADOOP_USER_NAME", "root");

        for (Path path : paths) {
            readLinesFromHDFS(path);
        }
    }
}

