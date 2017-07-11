package com.apicontract.spark.elasticsearch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.spark.rdd.api.java.JavaEsSpark.esRDD;
import static org.elasticsearch.spark.rdd.api.java.JavaEsSpark.saveToEs;

/**
 * Created by anki on 6/22/2017.
 */
public class ElasticSearchUpdateApp {

    private static final String ELASTICSEARCH_NODES_PROPERTY_NAME = "es.nodes";
    private static final String ELASTICSEARCH_NODES_WAN_ONLY_PROPERTY_NAME = "es.nodes.wan.only";
    private static final String FALSE_AS_STRING = "false";
    public static void main(String args[]){

        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("elasticsearch-app");
        conf.set("es.index.auto.create", "true");
        conf.set(ELASTICSEARCH_NODES_PROPERTY_NAME, "localhost:9200");
        conf.set("es.write.operation", "update");
        conf.set("es.mapping.id", "users_id");
        conf.set(ELASTICSEARCH_NODES_WAN_ONLY_PROPERTY_NAME, FALSE_AS_STRING);

        JavaSparkContext sc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession
                .builder()
                .appName(sc.appName())
                .config(sc.getConf())
                .getOrCreate();

        JavaPairRDD<String, Map<String,Object>> filteredList = esRDD(sc, "analytics/users", "?q=date:2014-06-12");
        List<Map<String, Object>> values = new ArrayList<Map<String, Object>>();
        filteredList.collect().forEach(value -> {value._2().put("users_id",value._1);value._2().put("name","anki"); values.add(value._2());});
        JavaRDD<Map<String, Object>> finalList = sc.parallelize(values);
        saveToEs(finalList,"analytics/users");
    }
}
