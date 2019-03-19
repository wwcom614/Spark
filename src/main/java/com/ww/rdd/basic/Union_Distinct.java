package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

// 对union来说，先去重，再合并-- A.distinct().union(B.distinct()).distinct()
// 这样可以减少shuffle的数据规模，从而降低开销。
public class Union_Distinct {

    public static void union_distinct() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("union_distinct");

        //创建好了程序的入口
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        List<Integer> list1 = Arrays.asList(1, 2, 3, 4, 2);
        List<Integer> list2 = Arrays.asList(3, 4, 5, 3);


        JavaRDD<Integer> list1RDD = sc.parallelize(list1);
        JavaRDD<Integer> list2RDD = sc.parallelize(list2);

        //并集union，操作本身是不去重的
        System.out.println("【union】"+ list1RDD.union(list2RDD).collect());
        //【union】[1, 2, 3, 4, 2, 3, 4, 5, 3]

        //如果想去重，那么先去重，再合并。这样可以减少shuffle的数据规模，从而降低开销
        System.out.println("【union + distinct】"+
                list1RDD.distinct().union(list2RDD.distinct()).distinct().collect());
        //【union + distinct】[4, 1, 5, 2, 3]

        sc.stop();
    }

    public static void main(String[] args) {
        union_distinct();
    }

}
