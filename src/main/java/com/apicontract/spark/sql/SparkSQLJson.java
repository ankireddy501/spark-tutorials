package com.apicontract.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
        spark.stop();
    }
}
