package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

import static org.apache.spark.sql.functions.sum;

public class PlayerUtils implements Serializable {

    Dataset<Row> pbpDF;

    public PlayerUtils(Dataset<Row> pbpDF) {
        this.pbpDF = pbpDF;
    }

    // Player code example: 1-J.HURTS
    public String getPlayerCode(String firstName, String lastName, int number) {
        return number + "-" + firstName.charAt(0) + "." + lastName.toUpperCase();
    }

    public long rushingYards(String playerCode) {
        System.out.println("1) rushingYards");
        Dataset<Row> rushDF = pbpDF.filter(pbpDF.col("Description").contains(playerCode).and(pbpDF.col("IsRush").equalTo(true)).and(pbpDF.col("IsNoPlay").equalTo(false)));
        System.out.println("2) rushingYards");
        Dataset<Row> sumDF = rushDF.agg(sum("Yards").alias("sum"));
        //sumDF.show();
        System.out.println("3) rushingYards");
        Row firstRow = sumDF.first();
        if (firstRow.getAs("sum") != null) {
            System.out.println("4) rushingYards");
            return firstRow.getAs("sum");
        }
        System.out.println("5) rushingYards");
        return 0;

    }

    public long passingYards(String playerCode){
        Dataset<Row> passDF = pbpDF.filter(pbpDF.col("Description").contains(playerCode + " PASS").and(pbpDF.col("IsNoPlay").equalTo(false)).and(pbpDF.col("IsPass").equalTo(true)));
        Dataset<Row> sumDF = passDF.agg(sum("Yards").alias("sum"));
        //sumDF.show();
        Row firstRow = sumDF.first();
        if (firstRow.getAs("sum") != null) {
            return firstRow.getAs("sum");
        }
        return 0;
    }

    public long receivingYards(String playerCode) {
        Dataset<Row> receptionDF = pbpDF.filter(pbpDF.col("Description").contains("TO " + playerCode).and(pbpDF.col("IsNoPlay").equalTo(false)).and(pbpDF.col("IsPass").equalTo(true)));
        Dataset<Row> sumDF = receptionDF.agg(sum("Yards").alias("sum"));
        //sumDF.show();
        Row firstRow = sumDF.first();
        if (firstRow.getAs("sum") != null) {
            return firstRow.getAs("sum");
        }
        return 0;
    }
}
