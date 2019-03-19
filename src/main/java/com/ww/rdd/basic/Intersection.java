package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class Intersection {

    public static void intersection() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("intersection");

        //创建好了程序的入口
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<Integer> listA = Arrays.asList(1, 2, 3, 4);
        List<Integer> listB = Arrays.asList(3, 4, 5);


        JavaRDD<Integer> listARDD = sc.parallelize(listA);
        JavaRDD<Integer> listBRDD = sc.parallelize(listB);
        //交集intersection
        JavaRDD<Integer> A_intersection_B = listARDD.intersection(listBRDD);
        System.out.println("A_intersection_B" + A_intersection_B.collect());

        sc.stop();
    }

    public static void main(String[] args) {
        intersection();
        //A_intersection_B[4, 3]
    }

}
