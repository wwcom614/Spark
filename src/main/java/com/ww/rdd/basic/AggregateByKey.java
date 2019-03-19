package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class AggregateByKey {

    public static void aggregateByKey() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("aggregateByKey");

        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        List<String> list = Arrays.asList("how are you", "thank you");
        JavaRDD<String> listRDD = sc.parallelize(list);
        //flatMap输入与输出是一对多的关系，FlatMapFunction的第二个参数表示返回值--可迭代的list
        JavaPairRDD<String, Integer> aggregateByKeyRDD = listRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            //把单词映射为(单词,1)的tuple2类型数据
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        })
/**
 * aggregateByKey多提供了一个函数，类似于Mapreduce的combine操作（就在map端本地执行reduce的操作）
 *  reduceBykey是aggregateByKey的简化版。
 * 第一个入参是每个key的初始value值。
 * 第二个入参是一个函数，类似于map-reduce中的本地聚合combine。
 * 第三个入参也是一个函数，类似于reduce的全局聚合。
 */
                .aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }, new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });

        System.out.println(aggregateByKeyRDD.collect());
        sc.stop();
    }

    public static void main(String[] args) {
        aggregateByKey();
        //[(are,1), (thank,1), (how,1), (you,2)]
    }

}
