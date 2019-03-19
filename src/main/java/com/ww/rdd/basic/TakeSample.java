package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;


public class TakeSample {

    public static void takeSample() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("takeSample");

        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        final List<Integer> list = Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        JavaRDD<Integer> listRDD = sc.parallelize(list);

        /*
takeSample:
对RDD中的集合内元素进行采样，
第一个参数withReplacement是true表示有放回取样，false表示无放回。
第二个参数num,随机取几个。
第三个参数是种子seed，为调试方便固定一组随机值使用。
*/
        List<Integer> takeSample = listRDD.takeSample(false, 3);

        for (Integer i : takeSample) {
            System.out.println(i);
        }

        sc.stop();
    }

    public static void main(String[] args) {
        takeSample();
        //4
        //6
        //2
    }

}
