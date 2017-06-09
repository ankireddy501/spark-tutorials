package com.apicontract.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

/**
 * Created by anki on 6/9/2017.
 */
public class SparkSQLJson {

    public static void main(String args[]){

        SparkSession spark = SparkSession
                .builder()
                .master("local[2]")
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> dataSet = spark.read().json("src/main/resources/people.json");
        dataSet.show();
        dataSet.select("name", "age").show();
        dataSet.select(col("name"), col("age")).show();
        dataSet.select(col("name").as("NAME"), col("age").as("AGE")).show();
        dataSet.select(col("name").as("NAME"), col("age").divide("2")).show();
        spark.stop();
    }
}
