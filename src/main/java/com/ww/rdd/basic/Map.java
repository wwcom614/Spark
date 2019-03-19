package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class Map {

    public static void map() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("map");

        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        List<String> list = Arrays.asList("Han Meimei", "Li Lei", "Xiao ming");
        JavaRDD<String> listRDD = sc.parallelize(list);
        //map的输入与输出是一对一的关系，Function的后一个参数R代表的是这个函数的返回值
        JavaRDD<String> map = listRDD.map(new Function<String, String>() {
            public String call(String str) throws Exception {
                return "Hello, " + str;
            }
        });

        map.foreach(new VoidFunction<String>() {
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });

        sc.stop();
    }

    public static void main(String[] args) {
        map();
        //Hello, Li Lei
        //Hello, Xiao ming
        //Hello, Han Meimei
    }

}
