package org.example;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import java.io.File;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Main {

    static final String PBP_FILEPATH = "./pbp-2024.csv";
    static final String ROSTER_FILEPATH = "./rosters_2024.csv";
    static final String SAVE_PATH = "./teamStats";


    private static void deleteDirectory(File dir) {
        File[] allContents = dir.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        dir.delete();
    }

    public static void main(String[] args) throws Exception{
        System.setProperty("hadoop.native.lib", "false");
        SparkExample sparkExample = new SparkExample(PBP_FILEPATH, ROSTER_FILEPATH);

        long rushYards = sparkExample.getPlayerRushYards("26-S.BARKLEY");
        System.out.println("Result: " + rushYards);

        Scanner scanner = new Scanner(System.in);
        scanner.nextLine();
        sparkExample.close();


       Dataset<Row> teamStats = sparkExample.getTeamStats("PHI");

        Gets rid of previous output
       File dir = new File(SAVE_PATH);

       if (dir.exists()) {
           deleteDirectory(dir);
       }
       dir.mkdir();

       //Saves dataframe to ./teamStats folder
       System.out.println("Saving");
       teamStats.repartition(1).write().mode("overwrite").option("header", true).csv(SAVE_PATH);
    }
}
