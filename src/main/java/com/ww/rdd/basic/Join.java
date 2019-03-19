package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Join {

    public static void join() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("join");

        //创建好了程序的入口
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<Tuple2<Integer, String>> tuple2NameList = Arrays.asList(
                new Tuple2<Integer, String>(1, "ww1"),
                new Tuple2<Integer, String>(2, "ww2"),
                new Tuple2<Integer, String>(3, "ww3"),
                new Tuple2<Integer, String>(4, "ww4")
        );

        List<Tuple2<Integer, Integer>> tuple2ScoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(2, 100),
                new Tuple2<Integer, Integer>(3, 99),
                new Tuple2<Integer, Integer>(4, 98),
                new Tuple2<Integer, Integer>(5, 97)
        );
        //tuple2类型的list转为RDD，需要使用parallelizePairs
        JavaPairRDD<Integer, String> tuple2NameListRDD = sc.parallelizePairs(tuple2NameList);
        JavaPairRDD<Integer, Integer> tuple2ScoreListRDD = sc.parallelizePairs(tuple2ScoreList);
        //innerjoin关联
        JavaPairRDD<Integer, Tuple2<String, Integer>> innerjoin = tuple2NameListRDD.join(tuple2ScoreListRDD);

        innerjoin.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
                System.out.println("No.：" + t._1);
                System.out.println("Name：" + t._2._1);
                System.out.println("Score：" + t._2._1);
            }
        });
        sc.stop();
    }

    public static void main(String[] args) {
        join();
        //No.：4
        //Name：ww4
        //Score：ww4
        //No.：2
        //Name：ww2
        //Score：ww2
        //No.：3
        //Name：ww3
        //Score：ww3
    }

}
