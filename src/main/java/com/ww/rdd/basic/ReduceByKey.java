package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class ReduceByKey {

    public static void reduceByKey() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("reduceByKey");

        //创建好了程序的入口
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<Tuple2<String, Integer>> tuple2List = Arrays.asList(
                new Tuple2<String, Integer>("Group1", 1),
                new Tuple2<String, Integer>("Group2", 2),
                new Tuple2<String, Integer>("Group1", 3),
                new Tuple2<String, Integer>("Group2", 4)
        );
        //tuple2类型的list转为RDD，需要使用parallelizePairs
        JavaPairRDD<String, Integer> tuple2ListRDD = sc.parallelizePairs(tuple2List);

        JavaPairRDD<String, Integer> reduceByKeyRDD = tuple2ListRDD.reduceByKey(
                //参数1和参数2是指的相同key时的原始value，参数3是相同key时的原始value做计算后返回的结果
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        reduceByKeyRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + "：" + stringIntegerTuple2._2);
            }
        });

        sc.stop();
    }

    public static void main(String[] args) {
        reduceByKey();
        //Group1：4
        //Group2：6
    }

}
