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

    private static void insertDataIntoMongoWHO(String collections, String report, List<String> countries, List<Integer> year,
                                               List<Integer> decade, List<String> quintil, List<Integer> population, List<Float> percentage,
                                               List<Float> cancer, List<Float> cardiovascular, List<Float> respiratory) {
        String connString = "mongodb+srv://josuemartinezcr:bd21234@bd2-p2.d37xqum.mongodb.net/?retryWrites=true&w=majority";
        MongoClient client = MongoClients.create(connString);
        MongoDatabase database = client.getDatabase("projectReports");
        MongoCollection<Document> collection = database.getCollection(collections);

        Document document = new Document();
        document.append("report", report);
        document.append("country", countries);
        document.append("year", year);
        document.append("decade", decade);
        document.append("quintil", quintil);
        document.append("population", population);
        document.append("percentage", percentage);
        document.append("cancer", cancer);
        document.append("cardiovascular", cardiovascular);
        document.append("respiratory", respiratory);

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
                    case "PromVicsXSexo":
                        sex.add(values[0]);
                        average.add(Float.parseFloat(values[1]));
                        break;
                    case "PromMaxMinXSubregion":
                        subregions.add(values[0]);
                        years.add(Integer.parseInt(values[1]));
                        average.add(Float.parseFloat(values[2]));
                        max.add(Float.parseFloat(values[3]));
                        min.add(Float.parseFloat(values[4]));
                        break;
                    case "PromMaxMinXSubregionXSexo":
                        subregions.add(values[0]);
                        sex.add(values[1]);
                        years.add(Integer.parseInt(values[2]));
                        average.add(Float.parseFloat(values[3]));
                        max.add(Float.parseFloat(values[4]));
                        min.add(Float.parseFloat(values[5]));
                        break;
                    case "PromMaxMinXRegionXSexoDec":
                        regions.add(values[0]);
                        sex.add(values[1]);
                        decades.add(Integer.parseInt(values[2]));
                        average.add(Float.parseFloat(values[3]));
                        max.add(Float.parseFloat(values[4]));
                        min.add(Float.parseFloat(values[5]));
                        break;
                    case "PromMaxMinXSubregionXSexoDec":
                        subregions.add(values[0]);
                        sex.add(values[1]);
                        decades.add(Integer.parseInt(values[2]));
                        average.add(Float.parseFloat(values[3]));
                        max.add(Float.parseFloat(values[4]));
                        min.add(Float.parseFloat(values[5]));
                        break;
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        insertDataIntoMongoDB(collection, reportTitle, countries, total, regions, subregions, average, max, min, sex, years, decades);
    }

    private static void readLinesFromWHO(Path path) throws URISyntaxException {

        String reportTitle = path.getParent().getName();
        String collection = path.getParent().getParent().getName();

        List<String> countries = new ArrayList<>();
        List<Integer> years = new ArrayList<>();
        List<Integer> decades = new ArrayList<>();
        List<String> quintil = new ArrayList<>();
        List<Integer> population = new ArrayList<>();
        List<Float> percentage = new ArrayList<>();
        List<Float> cancer = new ArrayList<>();
        List<Float> cardiovascular = new ArrayList<>();
        List<Float> respiratory = new ArrayList<>();

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
                    case "CanCarRespXPaisAnno":
                        countries.add(values[0]);
                        years.add(Integer.parseInt(values[1]));
                        cancer.add(Float.parseFloat(values[2]));
                        cardiovascular.add(Float.parseFloat(values[3]));
                        respiratory.add(Float.parseFloat(values[4]));
                        break;
                    case "PopulationByDecade":
                        countries.add(values[0]);
                        decades.add(Integer.parseInt(values[1]));
                        population.add(Integer.parseInt(values[2]));
                        break;
                    case "FertilityPercentage":
                        countries.add(values[0]);
                        decades.add(Integer.parseInt(values[1]));
                        percentage.add(Float.parseFloat(values[2]));
                        break;
                    case "FertilityPercentageQuin":
                        if (!values[1].equals(" ")) {
                            countries.add(values[0]);
                            quintil.add(values[1]);
                            decades.add(Integer.parseInt(values[2]));
                            percentage.add(Float.parseFloat(values[3]));
                        }
                        break;
                }
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        insertDataIntoMongoWHO(collection, reportTitle, countries, years,
                decades, quintil, population, percentage,
                cancer, cardiovascular, respiratory);
    }

    public static void main(String[] args) throws URISyntaxException {
        List<Path> paths = new ArrayList<>();
        List<Path> pathsWHO = new ArrayList<>();

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
        pathsWHO.add(new Path("/output/who/CanCarRespXPaisAnno/result.txt"));
        pathsWHO.add(new Path("/output/who/FertilityPercentage/result.txt"));
        pathsWHO.add(new Path("/output/who/FertilityPercentageQuin/result.txt"));
        pathsWHO.add(new Path("/output/who/PopulationByDecade/result.txt"));

        System.setProperty("HADOOP_USER_NAME", "root");

        for (Path path : paths) {
            readLinesFromHDFS(path);
        }

//        for (Path path : pathsWHO) {
//            readLinesFromWHO(path);
//        }
    }
}

