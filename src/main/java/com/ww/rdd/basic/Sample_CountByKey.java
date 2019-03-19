package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/*
Sample_CountByKey:
对RDD中的集合内元素进行采样，
第一个参数withReplacement是true表示有放回取样，false表示无放回。
第二个参数Fraction,一个大于0,小于或等于1的小数值,用于控制要读取的数据所占整个数据集的概率。
第三个参数是种子seed，为调试方便固定一组随机值使用。
*/
public class Sample_CountByKey {

    public static void sample_countByKey() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("sample_countByKey");

        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");


        List<Tuple2<String, Integer>> tuple2List = Arrays.asList(
                new Tuple2<String, Integer>("Group1", 1),
                new Tuple2<String, Integer>("Group2", 2),
                new Tuple2<String, Integer>("Group1", 3),
                new Tuple2<String, Integer>("Group1", 4),
                new Tuple2<String, Integer>("Group1", 5),
                new Tuple2<String, Integer>("Group3", 6),
                new Tuple2<String, Integer>("Group1", 7),
                new Tuple2<String, Integer>("Group3", 8),
                new Tuple2<String, Integer>("Group1", 9),
                new Tuple2<String, Integer>("Group1", 10),
                new Tuple2<String, Integer>("Group3", 11),
                new Tuple2<String, Integer>("Group1", 12),
                new Tuple2<String, Integer>("Group1", 13),
                new Tuple2<String, Integer>("Group1", 14),
                new Tuple2<String, Integer>("Group1", 15),
                new Tuple2<String, Integer>("Group3", 16),
                new Tuple2<String, Integer>("Group1", 17),
                new Tuple2<String, Integer>("Group1", 18),
                new Tuple2<String, Integer>("Group1", 19),
                new Tuple2<String, Integer>("Group3", 20)
        );

        JavaPairRDD<String, Integer> tuple2ListRDD = sc.parallelizePairs(tuple2List);

        //不放回采样，采样30%数据
        //可以利用sample+countByKey来排查数据倾斜问题
        Map<String, Long> sample_countByKey = tuple2ListRDD.sample(false, 0.3).countByKey();

        for(String key : sample_countByKey.keySet()){
            System.out.println(key + "：" + sample_countByKey.get(key));
        }
        // Group1：6
        // Group3：2

        sc.stop();
    }

    public static void main(String[] args) {
        sample_countByKey();
    }

}
