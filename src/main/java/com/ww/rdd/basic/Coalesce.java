package com.ww.rdd.basic;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/*
coalesce和reparition的使用场景实例：
filter之前数据量较大，分成100个partition也就是100个task处理，
经过filter之后，每个partition数据量变小，为节约计算资源，可以收缩partition数量，从而减少task数量。

coalesce: 如果原来有N个partition，需要转换成M个partition
N ---> M
1）N < M  分区变多，肯定需要将shuffle设置为true。
2）N > M 分区变少，且相差不多，N=1000 M=100  建议 shuffle=false 。
让父RDD和子RDD是窄依赖，避免不必要的shuffle损耗。
3）N >> M  分区大为减少，比如 n=100 m=1  建议shuffle设置为true，这样性能更好。
如果此时设置为false，父RDD和子RDD是窄依赖，他们同在一个stage中。造成任务并行度不够，从而速度缓慢。
*/
public class Coalesce {

    public static void coalesce() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("coalesce");

        //创建好了程序的入口
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("INFO");

        //模拟一个集合，使用并行化的方式创建出来一个RDD。
        final List<String> list = Arrays.asList("Han Meimei", "Li Lei", "Xiao ming");
        JavaRDD<String> listRDD = sc.parallelize(list);

        listRDD.coalesce(2,true).foreach(new VoidFunction<String>() {
            public void call(String t) throws Exception {
                System.out.println(t);
            }
        });

        sc.stop();
    }

    public static void main(String[] args) {
        coalesce();
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
