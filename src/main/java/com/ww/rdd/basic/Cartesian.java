package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Cartesian {

    public static void cartesian() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("union");

        //创建好了程序的入口
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<String> list1 = Arrays.asList("a", "b", "c");
        List<Integer> list2 = Arrays.asList(1, 2, 3, 4);


        JavaRDD<String> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> list2RDD = sc.parallelize(list2);
        //笛卡尔积
        JavaPairRDD<String, Integer> cartesianRDD = list1RDD.cartesian(list2RDD);

        System.out.println(cartesianRDD.collect());

        sc.stop();
    }

    public static void main(String[] args) {
        cartesian();
        //[(a,1), (a,2), (a,3), (a,4), (b,1), (b,2), (c,1), (c,2), (b,3), (b,4), (c,3), (c,4)]
    }

}
