package org.example;

import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.List;

public class SparkExample {
    SparkSession sparkSession;
    Dataset<Row> pbpDF;
    Dataset<Row> rosterDF;
    PlayerUtils playerUtils;

    public SparkExample(String pbpFilepath, String rosterFilepath) {
        System.out.println("1) init");
        SparkConf conf = new SparkConf()
                .setAppName("MyApp")
                .setMaster("local[4]");
        //The number in the square brackets determines how many simulated executor nodes we have
        //This only simulates the executors with more threads


//                .set("spark.driver.extraJavaOptions", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED")
//                .set("spark.executor.extraJavaOptions", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED");

        SparkContext sc = new SparkContext(conf);

        sparkSession = new SparkSession(sc);

        pbpDF = sparkSession.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(pbpFilepath)
                .repartition(8);

//        rosterDF= sparkSession.read()
//                .option("header", "true")
//                .option("inferSchema", "true")
//                .csv(rosterFilepath);

        playerUtils = new PlayerUtils(pbpDF);
    }

    public void close(){
        sparkSession.close();
    }

    public long getPlayerRushYards(String playerCode) {
        return playerUtils.rushingYards(playerCode);
    }

    public Dataset<Row> getTeamStats(String teamCode) {
        List<String> skillPositions = List.of("QB", "RB", "WR", "TE");
        Dataset<Row> teamRoster = rosterDF.filter(rosterDF.col("team").equalTo(teamCode).and(rosterDF.col("position").isInCollection(skillPositions)));

        Encoder<Row> rowEncoder = Encoders.javaSerialization(Row.class);

        Dataset<Row> playerRows = teamRoster.map(
                (MapFunction<Row, Row>) row -> {
                    String firstName = row.getAs("first_name");
                    String lastName = row.getAs("last_name");
                    String num = row.getAs("jersey_number");
                    int number;
                    try {
                        number = Integer.parseInt(num);
                    } catch (NumberFormatException e) {
                        number = 0;
                    }
                    return RowFactory.create(playerUtils.getPlayerCode(firstName, lastName, number), row.getAs("position"));
                },
                rowEncoder
        );

        // Row example: ["1-J.HURTS", "QB"]

        //playerLists.show();

        StructType schema = new StructType(new StructField[] {
                new StructField("playerCode", DataTypes.StringType, false, Metadata.empty()),
                new StructField("position", DataTypes.StringType, false, Metadata.empty()),
                new StructField("totalYards", DataTypes.LongType, false, Metadata.empty()),
                new StructField("passingYards", DataTypes.LongType, false, Metadata.empty()),
                new StructField("rushingYards", DataTypes.LongType, false, Metadata.empty()),
                new StructField("receivingYards", DataTypes.LongType, false, Metadata.empty())
        });

        List<Row> rowList = new ArrayList<>();

        for (Row playerRow : playerRows.collectAsList()) {
            String playerCode = playerRow.getString(0);
            long pass = playerUtils.passingYards(playerCode);
            long rush = playerUtils.rushingYards(playerCode);
            long receiving = playerUtils.receivingYards(playerCode);
            long totalYards = pass + rush + receiving;

            String position = playerRow.getString(1);

            Row row = RowFactory.create(playerCode,
                    position,
                    totalYards,
                    pass,
                    rush,
                    receiving
            );
            rowList.add(row);
        }

        Dataset<Row> teamStats = sparkSession.createDataFrame(rowList, schema);

        return teamStats.orderBy(teamStats.col("totalYards").desc());
    }

}
