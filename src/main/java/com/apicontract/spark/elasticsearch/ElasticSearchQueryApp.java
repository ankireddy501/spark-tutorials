package com.apicontract.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.spark.rdd.api.java.JavaEsSpark.esRDD;

/**
 * Created by anki on 6/22/2017.
 */
public class ElasticSearchQueryApp {

    private static final String ELASTICSEARCH_NODES_PROPERTY_NAME = "es.nodes";
    private static final String ELASTICSEARCH_NODES_WAN_ONLY_PROPERTY_NAME = "es.nodes.wan.only";
    private static final String FALSE_AS_STRING = "false";
    public static void main(String args[]){

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("elasticsearch-app");
        conf.set("es.index.auto.create", "true");
        conf.set(ELASTICSEARCH_NODES_PROPERTY_NAME, "localhost:9200");
        conf.set(ELASTICSEARCH_NODES_WAN_ONLY_PROPERTY_NAME, FALSE_AS_STRING);

        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession
                .builder()
                .appName(sc.appName())
                .config(sc.getConf())
                .getOrCreate();

        JavaPairRDD<String, Map<String,Object>> filteredList = esRDD(sc, "portal_analytics/active-users", "?q=date:2014-12-18");
        filteredList.collect().forEach(entry -> System.out.println(entry._1 + entry._2));



    }
}
