package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FlatMap {

    public static void flatMap() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("flatMap");

        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        List<String> list = Arrays.asList("how are you","thank you");
        JavaRDD<String> listRDD = sc.parallelize(list);
        //flatMap输入与输出是一对多的关系，FlatMapFunction的第二个参数表示返回值--可迭代的list
        JavaRDD<String> flatMap = listRDD.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String t) throws Exception {
                return Arrays.asList(t.split(" ")).iterator();
            }
        });

        flatMap.foreach(new VoidFunction<String>() {
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });

        sc.stop();
    }

    public static void main(String[] args) {
        flatMap();
        //how
        //are
        //you
        //thank
        //you
    }

}
