package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

public class TakeOrdered {

    public static void takeOrdered() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("takeOrdered");

        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        List<Integer> list = Arrays.asList(1, 3, 4, 2, 4, 9, 7, 8, 6);
        JavaRDD<Integer> listRDD = sc.parallelize(list);

        //RDD按值大小排序(必须实现comparable接口)，takeOrdered(N)升序输出RDD中的最小的前N个元素。
        System.out.println(listRDD.takeOrdered(3));

        sc.stop();
    }

    public static void main(String[] args) {
        takeOrdered();
        //[1, 2, 3]
    }

}
