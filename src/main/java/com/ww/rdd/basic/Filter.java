package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class Filter {

    public static void filter() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("filter");

        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        //Function的第二个参数表示返回值，是一个boolean值。
        JavaRDD<Integer> filter = listRDD.filter(new Function<Integer, Boolean>() {
            public Boolean call(Integer i) throws Exception {
                return i % 2 == 0;
            }
        });

        filter.foreach(new VoidFunction<Integer>() {
            public void call(Integer t) throws Exception {
                System.out.println(t);
            }
        });

        sc.stop();
    }

    public static void main(String[] args) {
        filter();
        //2
        //4
        //6
        //8
    }

}
