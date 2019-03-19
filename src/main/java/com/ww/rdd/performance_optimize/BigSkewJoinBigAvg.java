package com.ww.rdd.performance_optimize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.*;

/*
数据倾斜解决方案 之 大表join大表，转为sample找出少量倾斜key加前缀均匀join，最后union。
适用场景：两个RDD进行join的时候，如果数据量都比较大，无法采用BroadcastMapJoin解决。
此时sample一下两个RDD中的key分布情况。如果出现数据倾斜且是其中某一个RDD中的少数几个key的数据量过大，
而另一个RDD中的所有key都分布比较均匀，那么本解决方案。
如果导致倾斜的key特别多的话，比如成千上万个key都导致数据倾斜，那么这种方式也不适合。
实现原理：对于join导致的数据倾斜，如果只是某几个key导致了倾斜，可以将少数几个key分拆成独立RDD，
并附加随机前缀打散成n份去进行join，此时这几个key对应的数据就不会集中在少数几个task上，而是分散到多个task进行join了。
只需要针对少数倾斜key对应的数据进行扩容n倍，不是对全量数据进行扩容。避免占用过多内存。
*/
public class BigSkewJoinBigAvg {
    public static void bigSkewJoinBigAvg() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("bigSkewJoinBigAvg");

        //创建好了程序的入口
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        final List<Tuple2<Integer, String>> bigAvgTableList = Arrays.asList(
                new Tuple2<Integer, String>(1, "ww1"),
                new Tuple2<Integer, String>(2, "ww2")
        );

        final List<Tuple2<Integer, Double>> bigSkewTableList = Arrays.asList(
                new Tuple2<Integer, Double>(1, 100.00),
                new Tuple2<Integer, Double>(2, 95.52),
                new Tuple2<Integer, Double>(1, 98.37),
                new Tuple2<Integer, Double>(1, 66.86),
                new Tuple2<Integer, Double>(2, 55.92),
                new Tuple2<Integer, Double>(1, 44.33),
                new Tuple2<Integer, Double>(3, 33.12),
                new Tuple2<Integer, Double>(1, 22.98),
                new Tuple2<Integer, Double>(1, 11.04),
                new Tuple2<Integer, Double>(2, 60.88)
        );

        //tuple2类型的list转为RDD，需要使用parallelizePairs
        JavaPairRDD<Integer, String> bigAvgTableRDD = sc.parallelizePairs(bigAvgTableList);
        JavaPairRDD<Integer, Double> bigSkewTableRDD = sc.parallelizePairs(bigSkewTableList);

        //假设通过sample已经找到了倾斜的keys，如下针对这些keys的数据进行操作
        // 1.将key分布均匀的大表RDD的key加前缀扩容。
        JavaPairRDD<String, String> extendBigAvgTableRDD = bigAvgTableRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, String>, String, String>() {
            public Iterator<Tuple2<String, String>> call(Tuple2<Integer, String> t) throws Exception {
                ArrayList<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                for (int i = 0; i < 100; i++) {
                    list.add(new Tuple2<String, String>(i + "_" + t._1, t._2));
                }
                return list.iterator();
            }
        });

        // 2.将key分布倾斜的大表RDD的key加随机前缀扩容。
        JavaPairRDD<String, Double> extendBigSkewTableRDD = bigSkewTableRDD.mapToPair(new PairFunction<Tuple2<Integer, Double>, String, Double>() {
            public Tuple2<String, Double> call(Tuple2<Integer, Double> t) throws Exception {
                Random random = new Random();
                int prefix = random.nextInt(100);
                return new Tuple2<String, Double>(prefix + "_" + t._1, t._2);
            }
        });

        //3.原本倾斜的keys经过前缀扩容后不再倾斜，join，然后去除前缀看结果
        extendBigSkewTableRDD.join(extendBigAvgTableRDD).foreach(new VoidFunction<Tuple2<String, Tuple2<Double, String>>>() {
            public void call(Tuple2<String, Tuple2<Double, String>> t) throws Exception {
                System.out.print("[" + t._1.split("_")[1] + ":(" + t._2._1 +"," + t._2._2 + ")];" );
            }
        });
        //[1:(22.98,ww1)];[1:(100.0,ww1)];[2:(55.92,ww2)];[2:(95.52,ww2)];[1:(11.04,ww1)];[2:(60.88,ww2)];[1:(66.86,ww1)];[1:(98.37,ww1)];[1:(44.33,ww1)];

        sc.stop();
    }

    public static void main(String[] args) {
        bigSkewJoinBigAvg();
    }

}
