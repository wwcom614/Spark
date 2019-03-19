package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class GroupByKey {

    public static void groupByKey() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("groupByKey");

        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<Tuple2<String, String>> tuple2List = Arrays.asList(
                new Tuple2<String, String>("Group1", "001"),
                new Tuple2<String, String>("Group2", "002"),
                new Tuple2<String, String>("Group1", "003"),
                new Tuple2<String, String>("Group3", "004")
        );
        //tuple2类型的list转为RDD，需要使用parallelizePairs
        JavaPairRDD<String, String> tuple2ListRDD = sc.parallelizePairs(tuple2List);
        //RDD在groupByKey后，会生成分组key，和可Iterable的集合value
        JavaPairRDD<String, Iterable<String>> groupByKeyRDD = tuple2ListRDD.groupByKey();

        //把groupByKey后的结果转换成List<Tuple2<String, String>>，打印出来
        //主要是要将可Iterable的集合value转换为一个String，并与分组key对应起来
        List<Tuple2<String, String>> resultList = groupByKeyRDD.map(new Function<Tuple2<String,Iterable<String>>, Tuple2<String, String>>() {

            public Tuple2<String, String> call(Tuple2<String, Iterable<String>> stringIterableTuple2) throws Exception {
                String key = stringIterableTuple2._1;
                StringBuffer sb = new StringBuffer();
                for(String iter: stringIterableTuple2._2){
                    sb.append(iter).append(" ");
                }
                return new Tuple2<String, String>(key, sb.toString().trim());
            }
        }).collect();

       for(Tuple2<String ,String>tuple2: resultList){
           System.out.println(tuple2._1 + "：" + tuple2._2);
       }

        sc.stop();
    }

    public static void main(String[] args) {
        groupByKey();
        //Group1：001 003
        //Group3：004
        //Group2：002
    }

}
