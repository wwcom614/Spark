package com.ww.rdd.performance_optimize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/*
数据倾斜的解决方案 之 两阶段聚合（局部聚合+全局聚合）
适用场景：对RDD执行reduceByKey等聚合类shuffle算子或者在Spark SQL中使用group by语句进行分组聚合时。
实现原理：将原本相同的key通过附加随机前缀的方式，变成多个不同的key，
就可以让原本被一个task处理的数据分散到多个task上去做局部聚合，进而解决单个task处理数据量过多的问题。
接着去除掉随机前缀，再次进行全局聚合，就可以得到最终的结果。
*/
public class Agg2PC {

    public static void agg2PC() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("agg2PC");

        //创建好了程序的入口
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("INFO");

        List<Tuple2<String, Long>> tupleList = Arrays.asList(
                new Tuple2<String, Long>("ww1", 1L),
                new Tuple2<String, Long>("ww1", 2L),
                new Tuple2<String, Long>("ww1", 3L),
                new Tuple2<String, Long>("ww1", 4L),
                new Tuple2<String, Long>("ww1", 5L),
                new Tuple2<String, Long>("ww1", 6L),
                new Tuple2<String, Long>("ww1", 7L),
                new Tuple2<String, Long>("ww1", 8L),
                new Tuple2<String, Long>("ww1", 9L),
                new Tuple2<String, Long>("ww2", 10L)
        );
        //数据key倾斜的RDD
        JavaPairRDD<String, Long> dataSkewRDD = sc.parallelizePairs(tupleList);

        //第一步：给key倾斜的dataSkewRDD中每个key都打上一个随机前缀
        JavaPairRDD<String, Long> randomPrefixRdd = dataSkewRDD.mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {
            private static final long serialVersionUID = 1L;

            public Tuple2<String, Long> call(Tuple2<String, Long> t) throws Exception {
                Random random = new Random();
                int prefix = random.nextInt(10);
                return new Tuple2<String, Long>(prefix + "_" + t._1, t._2);
            }
        });

        //第二步：对打上随机前缀的key不再倾斜的randomPrefixRdd进行局部聚合
        JavaPairRDD<String, Long> localAggRdd = randomPrefixRdd.reduceByKey(new Function2<Long, Long, Long>() {
            private static final long serialVersionUID = 1L;

            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println("【localAggRdd】："+ localAggRdd.collect());
        //【localAggRdd】：[(8_ww1,2), (6_ww1,5), (1_ww2,10), (4_ww1,16), (1_ww1,1), (9_ww1,21)]

        //第三步：局部聚合后，去除localAggRdd中每个key的随机前缀
        JavaPairRDD<String, Long> removeRandomPrefixRdd = localAggRdd.mapToPair(new PairFunction<Tuple2<String, Long>, String, Long>() {
            private static final long serialVersionUID = 1L;

            public Tuple2<String, Long> call(Tuple2<String, Long> t) throws Exception {
                return new Tuple2<String, Long>(t._1.split("_")[1], t._2);
            }
        });

        //第四步：对去除了随机前缀的removeRandomPrefixRdd进行全局聚合
        JavaPairRDD<String, Long> globalAggRdd = removeRandomPrefixRdd.reduceByKey(new Function2<Long, Long, Long>() {
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println("【globalAggRdd】："+ globalAggRdd.collect());
        //【globalAggRdd】：[(ww2,10), (ww1,45)]

        sc.stop();
    }

    public static void main(String[] args) {
        agg2PC();

    }
}


