package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MapPartitionsWithIndex {

    public static void mapPartitionsWithIndex() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("mapPartitionsWithIndex");

        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> listRDD = sc.parallelize(list,3);


        //mapPartitionsWithIndex：与mapPartitions基本相同，
        // 只是处理函数的参数是一个二元元组，
        // 元组的第一个元素是当前处理分区的index，
        // 元组的第二个元素是当前处理分区元素组成的iterator
        listRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            public Iterator<String> call(Integer index, Iterator<Integer> vIter) throws Exception {
                ArrayList<String> list = new ArrayList<String>();
                while (vIter.hasNext()){
                    String kv = index + "_" + vIter.next();
                    list.add(kv);
                }
                return list.iterator();
            }
        },true)

                .foreach(new VoidFunction<String>() {
                    public void call(String s) throws Exception {
                        System.out.println(s);
                    }
                });

        sc.stop();
    }

    public static void main(String[] args) {
        mapPartitionsWithIndex();
        //1_4
        //0_1
        //1_5
        //1_6
        //0_2
        //0_3
        //2_7
        //2_8
        //2_9
    }

}
