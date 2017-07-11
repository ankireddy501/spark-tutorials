package com.apicontract.spark.elasticsearch;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by anki on 6/22/2017.
 */
public class ElasticSearchApp {

    private static final String ELASTICSEARCH_NODES_PROPERTY_NAME = "es.nodes";
    private static final String ELASTICSEARCH_NODES_WAN_ONLY_PROPERTY_NAME = "es.nodes.wan.only";
    private static final String FALSE_AS_STRING = "false";

    public static void main(String args[]){

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("elasticsearch-app");
        conf.set("es.index.auto.create", "true");
        conf.set(ELASTICSEARCH_NODES_PROPERTY_NAME, "localhost:9200");
        conf.set(ELASTICSEARCH_NODES_WAN_ONLY_PROPERTY_NAME, FALSE_AS_STRING);
        JavaSparkContext sc = new JavaSparkContext(conf);
        Map<String, String> numbers = new HashMap<String, String>();
        numbers.put("one", "1");
        numbers.put("two", "2");
        Map<String, String> airports =  new HashMap<String, String>();
        airports.put("OTP", "Otopeni");
        airports.put("SFO", "San Fran");

        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        list.add(numbers);
        list.add(airports);

        JavaRDD<Map<String, String>> javaRDD = sc.parallelize(list);
        JavaEsSpark.saveToEs(javaRDD, "spark/docs");

    }
}
