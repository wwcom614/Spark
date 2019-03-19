package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/*
MapPartitions:
该函数和map函数类似，只不过映射函数的参数由RDD中的每一个元素变成了RDD中每一个分区的迭代器。
如果在映射的过程中需要频繁创建额外的对象，使用mapPartitions要比map高效的多。
比如，将RDD中的所有数据通过JDBC连接写入数据库，
如果使用map函数，可能要为每一个元素都创建一个connection，这样开销很大，
如果使用mapPartitions，那么只需要针对每一个分区建立一个connection。
使用时务必考虑每个partition数据规模，过大有可能出现内存容量问题。
*/
public class MapPartitions {

    public static void mapPartitions() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("mapPartitions");

        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("INFO");

        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        final List<String> list = Arrays.asList("Han Meimei", "Li Lei", "Xiao ming");
        //为验证mapPartitions，parallelize时设置生成2个分区
        JavaRDD<String> listRDD = sc.parallelize(list,2);
        //mapPartitions的入参是FlatMapFunction
        JavaRDD<String> mapPartitions = listRDD.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

            public Iterator<String> call(Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while (iterator.hasNext()){
                    list.add("hello, "+ iterator.next());
                }
                return list.iterator();
            }
        });

        mapPartitions.foreach(new VoidFunction<String>() {
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });

        sc.stop();
    }

    public static void main(String[] args) {
        mapPartitions();
        //INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0, localhost, executor driver, partition 0, PROCESS_LOCAL, 7902 bytes)
        //INFO TaskSetManager: Starting task 1.0 in stage 0.0 (TID 1, localhost, executor driver, partition 1, PROCESS_LOCAL, 7902 bytes)
        //INFO Executor: Running task 0.0 in stage 0.0 (TID 0)
        //INFO Executor: Running task 1.0 in stage 0.0 (TID 1)
        //Hello, Li Lei
        //Hello, Xiao ming
        //Hello, Han Meimei
    }

}
