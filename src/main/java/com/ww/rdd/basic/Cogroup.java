package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/*
Cogroup:
根据两个要进行合并的两个RDD操作,生成一个CoGroupedRDD的实例,
这个RDD的返回结果是把相同的key中两个RDD分别进行合并操作,最后返回的RDD的value是一个Pair的实例,
这个实例包含两个Iterable的值,第一个值表示的是RDD1中相同KEY的值,第二个值表示的是RDD2中相同key的值.
*/
public class Cogroup {

    public static void cogroup() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("cogroup");

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
        //cogroup是把两个集合相同key的key作为key，value是两个集合对应key的value
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = tuple2NameListRDD.cogroup(tuple2ScoreListRDD);


        System.out.println(cogroup.collectAsMap());

        sc.stop();
    }

    public static void main(String[] args) {
        cogroup();
        //{2=([ww2],[100]), 5=([],[97]), 4=([ww4],[98]), 1=([ww1],[]), 3=([ww3],[99])}
    }

}
