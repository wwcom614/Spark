package com.ww.rdd.performance_optimize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
数据倾斜的解决方案 之 小表join大表转为小表broadcast+map大表实现。
适用场景：在对RDD使用join类操作，或者是在Spark SQL中使用join语句时，
并且join操作中的一个RDD或表的数据量比较小（比如几百M或者一两G）。
实现原理：普通的join是会走shuffle过程的，而一旦shuffle，
就相当于会将相同key的数据拉取到一个shuffle read task中再进行join，此时就是reduce join。
但是如果一个RDD比较小，则可以采用广播小RDD全量数据+map算子来实现与join同样的效果，也就是map join，
将较小RDD中的数据直接通过collect算子拉取到Driver端的内存中来，然后对其创建一个Broadcast变量；
接着对另外一个RDD执行map类算子，在算子函数内，从Broadcast变量中获取较小RDD的全量数据，
与当前RDD的每一条数据按照连接key进行比对，如果连接key相同的话，那么就将两个RDD的数据用需要的方式连接起来。
此时不会发生shuffle操作，也就不会发生数据倾斜。
 */
public class BroadcastMapJoin {
    public static void broadcastMapJoin() {
        //创建一个sparkConf对象
        SparkConf conf = new SparkConf();
        //在本地运行,设置setmaster参数为local
        //如果不设置，默认为在集群模式下运行。
        conf.setMaster("local[2]");
        //设置任务名称。
        conf.setAppName("broadcastMapJoin");

        //创建好了程序的入口
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        final List<Tuple2<Integer, String>> littleTableList = Arrays.asList(
                new Tuple2<Integer, String>(1, "ww1"),
                new Tuple2<Integer, String>(2, "ww2")
        );

        final List<Tuple2<Integer, Double>> bigTableList = Arrays.asList(
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
        JavaPairRDD<Integer, String> littleTableRDD = sc.parallelizePairs(littleTableList);
        JavaPairRDD<Integer, Double> bigTableRDD = sc.parallelizePairs(bigTableList);

        //一个RDD或表的数据量比较小，比如几百MB或者1~2GB
        //首先，将要join的数据量比较小的littleTableRDD的数据，collect到Driver中。
        // 小表数据量要小的原因1：因为collect到Driver单机上，内存一定要够用
        List<Tuple2<Integer, String>> collectlittleTable = littleTableRDD.collect();

        //然后使用Spark的广播功能，将该小数据量RDD的数据转换成广播变量，发送到每个Executor中存储。
        //小表数据量要小的原因2：广播后，该小表在每个Executor都要占用内存空间，且广播也有网络传输性能开销。
        //注意广播变量是final类型
        final Broadcast<List<Tuple2<Integer, String>>> broadcastLittleTable = sc.broadcast(collectlittleTable);

        //PairFunction第1个入参Tuple2<Integer, Double>是指的bigTableRDD，第2个入参Integer是指的2个rdd要join的key，
        // 第3个入参Tuple2<String, Double>>是指的join之后，同一个key的littleTableRDD的value和bigTableRDD的value
        JavaPairRDD<Integer, Tuple2<String, Double>> resultRDD = bigTableRDD.mapToPair(new PairFunction<Tuple2<Integer, Double>, Integer, Tuple2<String, Double>>() {
            public Tuple2<Integer, Tuple2<String, Double>> call(
                    Tuple2<Integer, Double> bigTableTuple) throws Exception {
                Map<Integer,String> littleTableMap = new HashMap<Integer, String>();
                //从Executor获取被广播的小表数据，存入一个Map中，便于后面join操作
                for(Tuple2<Integer,String> record: broadcastLittleTable.value()){
                    littleTableMap.put(record._1, record._2);
                }
                //返回值key
                Integer key = bigTableTuple._1;
                //返回值join之后，上述key的小表对应value值
                String littleTableValue = littleTableMap.get(key);
                //返回值join之后，上述key的大表对应value值
                Double bigTableValue = bigTableTuple._2;
                return new Tuple2<Integer, Tuple2<String, Double>>(key, new Tuple2<String, Double>(littleTableValue,bigTableValue));
            }
        });

        System.out.println(resultRDD.collect());
        //[(1,(ww1,100.0)), (2,(ww2,95.52)), (1,(ww1,98.37)), (1,(ww1,66.86)), (2,(ww2,55.92)), (1,(ww1,44.33)), (3,(null,33.12)), (1,(ww1,22.98)), (1,(ww1,11.04)), (2,(ww2,60.88))]
        sc.stop();
    }

    public static void main(String[] args) {
        broadcastMapJoin();
    }

}
