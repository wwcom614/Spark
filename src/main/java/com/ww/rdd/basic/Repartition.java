package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/*
reparition是coalesce shuffle为true的简易实现，肯定会进行shuffle。
*/
public class Repartition {

    public static void repartition() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("repartition");

        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("INFO");

        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        final List<String> list = Arrays.asList("Han Meimei", "Li Lei", "Xiao ming");
        JavaRDD<String> listRDD = sc.parallelize(list);

        listRDD.repartition(2).foreach(new VoidFunction<String>() {
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });

        sc.stop();
    }

    public static void main(String[] args) {
        repartition();
        //INFO TaskSchedulerImpl: Adding task set 1.0 with 2 tasks
        //INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 2, localhost, executor driver, partition 0, ANY, 7938 bytes)
        //INFO TaskSetManager: Starting task 1.0 in stage 1.0 (TID 3, localhost, executor driver, partition 1, ANY, 7938 bytes)
        //INFO Executor: Running task 0.0 in stage 1.0 (TID 2)
        //INFO Executor: Running task 1.0 in stage 1.0 (TID 3)
        //Hello, Li Lei
        //Hello, Xiao ming
        //Hello, Han Meimei
    }

}
