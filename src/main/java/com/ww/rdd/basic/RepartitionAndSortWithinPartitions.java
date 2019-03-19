package com.ww.rdd.basic;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class RepartitionAndSortWithinPartitions {

    public static void repartitionAndSortWithinPartitions() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("repartitionAndSortWithinPartitions");

        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> listRDD = sc.parallelize(list);
        final Random random = new Random();
        JavaPairRDD<Integer, Integer> mapToPair = listRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            public Tuple2<Integer, Integer> call(Integer key) throws Exception {
                return new Tuple2<Integer, Integer>(key, random.nextInt(10));
            }
        });

        //repartitionAndSortWithinPartitions
        //可根据自定义partitioner方法对RDD进行分区，并且在每个分区中对key进行排序；
        //与sortByKey算子对比，repartitionAndSortWithinPartitions比先分区，然后在每个分区中排序效率高，因为它将排序融入到shuffle阶段。
        JavaPairRDD<Integer, Integer> repartitionAndSortWithinPartitionsRDD =
                mapToPair.repartitionAndSortWithinPartitions(new Partitioner() {
            @Override
            public int getPartition(Object key) {
                return key.toString().hashCode() % 3;
            }

            @Override
            public int numPartitions() {
                return 3;
            }
        });


        repartitionAndSortWithinPartitionsRDD.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            public void call(Tuple2<Integer, Integer> t) throws Exception {
                System.out.println(t._1 + ":" + t._2);
            }
        });

        sc.stop();
    }

    public static void main(String[] args) {
        repartitionAndSortWithinPartitions();
        //3:1
        //6:3
        //9:7

        //1:5
        //4:3
        //7:1

        //2:3
        //5:5
        //8:3
    }

}
