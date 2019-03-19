package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class Distinct {

    public static void distinct() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("distinct");

        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        List<Integer> list = Arrays.asList(1, 2, 3, 3, 1, 2, 2, 3, 3, 3, 1, 1, 2);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        //Function的第二个参数表示返回值，是一个boolean值。
        JavaRDD<Integer> distinct = listRDD.distinct();

        distinct.foreach(new VoidFunction<Integer>() {
            public void call(Integer t) throws Exception {
                System.out.println(t);
            }
        });

        sc.stop();
    }

    public static void main(String[] args) {
        distinct();
        //2
        //1
        //3
    }

}
