package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class SortByKey {

    public static void sortByKey() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("sortByKey");

        //创建好了程序的入口
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<Tuple2<Integer, String>> tuple2List = Arrays.asList(
                new Tuple2<Integer, String>(66, "No3"),
                new Tuple2<Integer, String>(88, "No2"),
                new Tuple2<Integer, String>(99, "No1"),
                new Tuple2<Integer, String>(55, "No4")
        );
        //tuple2类型的list转为RDD，需要使用parallelizePairs
        JavaPairRDD<Integer, String> tuple2ListRDD = sc.parallelizePairs(tuple2List);
        //sortByKey(false)按key倒序排列
        System.out.println(tuple2ListRDD.sortByKey(false).collect());

        sc.stop();
    }

    public static void main(String[] args) {
        sortByKey();
        //[(99,No1), (88,No2), (66,No3), (55,No4)]
    }

}
