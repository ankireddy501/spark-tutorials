package com.apicontract.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.spark.rdd.api.java.JavaEsSpark.saveToEs;

/**
 * Created by anki on 7/24/2017.
 */
public class ElasticSearchCreateOrUpdateApp {

    private static final String ELASTICSEARCH_NODES_PROPERTY_NAME = "es.nodes";
    private static final String ELASTICSEARCH_NODES_WAN_ONLY_PROPERTY_NAME = "es.nodes.wan.only";
    private static final String FALSE_AS_STRING = "false";

    public static void main(String args[]){
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("elasticsearch-app");
            conf.set("es.index.auto.create", "true");
            conf.set(ELASTICSEARCH_NODES_PROPERTY_NAME, "localhost:9200");
            conf.set("es.write.operation", "upsert");
            conf.set("es.index.read.missing.as.empty", "true");
            conf.set("es.mapping.id", "id");
            conf.set(ELASTICSEARCH_NODES_WAN_ONLY_PROPERTY_NAME, FALSE_AS_STRING);

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();
        Map<String, Object> entry = new HashMap<String, Object>();
            entry.put("id","2017-07-24"); entry.put("count",13);
            values.add(entry);

        JavaRDD<Map<String, Object>> finalList = sc.parallelize(values);
        saveToEs(finalList,"analytics/users");

        values = new ArrayList<Map<String, Object>>();
        entry = new HashMap<String, Object>();
        entry.put("id","2017-07-24"); entry.put("count",15);
        values.add(entry);

        finalList = sc.parallelize(values);
        saveToEs(finalList,"analytics/users");
    }
}
