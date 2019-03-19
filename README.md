领导安排我给科室同事培训下Spark，前期生产实践的经验一直没顾得上总结，借此机会做个回顾，也算给自己做个积累。  

1.rdd.basic包：从Spark最基础的常用transformations和actions算子开始，java、scala编码，以及一些生产环境中的实践和经验说明。    
2.rdd.performance_optimize包：生产中避免数据倾斜、性能调优的的java、scala编码总结。  
3.rdd.statproject包：使用RDD写了个统计项目，结合生产中的常用业务场景，如何组合RDD算子实现。     
4.sparksql.statproject包：使用SparkSQL写了个统计项目，生产中对网站访问日志统计分析部分完整代码。
特意构造并实践了生产中的线程安全日期转换，IP映射城市，cache、foreachPartition、批量提交入库等性能提升代码。   
5.sparkstreaming包：主要码了rdd算子之外的流式计算常用算子，累计计数UpdateStateByKey和MapWithState，滑动窗口ReduceByKeyAndWindow，
自定义算子Transform、分区读取批量入库foreachPartition、与Kakfa集成以及Driver高可用KafkaIntegration。   

##  RDD        
###  basic包    
Spark支持两种RDD操作：transformation和action：    
1.transformation操作会针对已有的RDD创建一个新的RDD。特点是lazy，不会触发spark程序执行。        
2.action则主要是对RDD进行最后的操作，比如遍历、reduce、保存到文件等，并可以返回结果给Driver程序。
每个action操作执行，会触发一个spark job运行，从而触发这个action之前所有的transformation的执行。  
3.注意persist或cache也是lazy的。  
4.这种设计可以避免transformation产生过多中间结果，引起内存、网络IO问题。   
5.java.com.ww.rdd.basic包下java实践。  
6.scala.com.ww.rdd.basic包下scala实践。    
7.可能会触发shuffle操作的算子：distinct、groupByKey、reduceByKey、aggregateByKey、join、cogroup、repartition等。  
-  Map.java：  
生产中如果明确key不会变的 map，会使用mapValues替代，这样可以保证Spark不会shuffle。   
A.map{case (A, ((B, C), (D, E))) => (A, (B, C, E))}   
替代成：   
A.mapValues{case ((B, C), (D, E)) => (B, C, E)}   

-  Filter.java：   
生产中一般使用filter对大的 RDD 先进行过滤“瘦身”，然后再做其他重量级操作，例如join等。   

-  FlatMap.java：  
将RDD集合中的数据打平后处理。   

-  GroupByKey、ReduceByKey：  
生产中聚合操作优先使用reduceByKey，而不是groupByKey。  
因为reduceByKey会在当前节点(local)中做reduce操作，也就是说，会在shuffle前，尽可能地减小数据量。
而 groupByKey 会不做任何处理而直接去 shuffle。    
当然，有一些场景下，功能上二者并不能互相替换。
因为reduceByKey要求参与运算的value，要和输出的value类型一样，但是groupByKey则没有这个要求。   
还有一些类似的xxxByKey操作，都比groupByKey好，比如foldByKey和aggregateByKey。  

-  SortByKey.java：  
sortByKey(false)按key倒序排列，sortByKey(true)按key顺序排列。  

-  Join.java：  
inner join关联。   

-  Cogroup.java： 
根据两个要进行合并的两个RDD操作,生成一个CoGroupedRDD的实例，
这个RDD的返回结果是把相同的key中两个RDD分别进行合并操作,最后返回的RDD的value是一个Pair的实例,
这个实例包含两个Iterable的值,第一个值表示的是RDD1中相同KEY的值,第二个值表示的是RDD2中相同key的值。  

-  Union_Distinct.java： 
并集union，操作本身是不去重的。   
如果想去重，那么先去重，再合并：A.distinct().union(B.distinct()).distinct()。
这样可以减少shuffle的数据规模，从而降低开销。  

-  Intersection.java，Cartesian.java：  
交集intersection和笛卡尔积cartesian。  

-  MapPartitions.java：   
该函数和map函数类似，只不过映射函数的参数由RDD中的每一个元素变成了RDD中每一个分区的迭代器。  
如果在映射的过程中需要频繁创建额外的对象，使用mapPartitions要比map高效的多。   
比如，将RDD中的所有数据通过JDBC连接写入数据库：   
如果使用map函数，可能要为每一个元素都创建一个connection，这样开销很大。  
如果使用mapPartitions，那么只需要针对每一个分区建立一个connection。  
使用时务必考虑每个partition数据规模，过大有可能出现OOM(内存溢出)问题。  

-  Coalesce.java，Repartition.java：  
1.使用场景实例：  
filter之前数据量较大，分成100个partition也就是100个task处理，
经过filter之后，每个partition数据量变小，为节约计算资源，可以收缩partition数量，从而减少task数量。   
2.coalesce：  
如果原来有N个partition，需要转换成M个partitions，N ---> M：   
1）N < M，分区变多，肯定需要将shuffle设置为true。   
2）N > M，分区变少，且相差不多，N=1000 M=100  建议 shuffle=false。让父RDD和子RDD是窄依赖，避免不必要的shuffle损耗。  
3）N >> M，分区大为减少，比如 n=100 m=1  建议shuffle设置为true，有shuffle，但可以增加计算并行度，这样性能更好。  
如果此时设置为false，父RDD和子RDD是窄依赖，他们同在一个stage中。造成任务并行度不够，从而速度缓慢。   
3.reparition.java： 
是coalesce的shuffle为true的简易实现，肯定会进行shuffle。   

-  Sample_CountByKey.java：  
对RDD中的集合内元素进行采样，
第一个参数withReplacement是true表示有放回取样，false表示无放回。
第二个参数Fraction,一个大于0,小于或等于1的小数值,用于控制要读取的数据所占整个数据集的概率。 
第三个参数是种子seed，为调试方便固定一组随机值使用。  
可以利用sample+countByKey来排查数据倾斜问题：pairs.sample(false, 0.1).countByKey()。  

-  AggregateByKey.java：  
aggregateByKey多提供了一个函数，类似于Mapreduce的combine操作(就在map端本地执行reduce的操作)，
reduceBykey是aggregateByKey的简化版。  
第一个入参是每个key的初始value值。  
第二个入参是一个函数，类似于map-reduce中的本地聚合combine。  
第三个入参也是一个函数，类似于reduce的全局聚合。   

-   mapPartitionsWithIndex.java：  
与mapPartitions基本相同，只是处理函数的参数是一个二元元组，  
元组的第一个元素是当前处理分区的index，  
元组的第二个元素是当前处理分区元素组成的iterator。  

-   RepartitionAndSortWithinPartitions.java
repartitionAndSortWithinPartitions是Spark官网推荐的一个算子，
官方建议，如果需要在repartition重分区之后，还要进行排序，
建议直接使用repartitionAndSortWithinPartitions算子。   
因为该算子可以一边进行重分区的shuffle操作，一边进行排序。
shuffle与sort两个操作同时进行，比先shuffle再sort来说，性能可能是要高的。   
  
-  Reduce.java    
对RDD做聚合计算，输出计算结果。   

-  Collect.java      
一般调试时谨慎使用，生产上基本不用，因为collect会把整个rdd的数据获取到Driver，数据量过大有可能内存溢出。

-  Take.java      
生产上一般使用take(N)，输出RDD的前N个元素看一下。   

-  TakeOrdered.java      
RDD按值大小排序(必须实现comparable接口)，takeOrdered(N)升序输出RDD中的最小的前N个元素。  

-  Top.java     
RDD按值大小排序(必须实现comparable接口)，top(N)降序输出RDD中的最大的前N个元素。  

-  Count.java      
看RDD中有多少个元素。   

-  SaveAsTextFile.java    
保存文件。   

-  TakeSample.java：  
对RDD中的集合内元素进行采样，  
第一个参数withReplacement是true表示有放回取样，false表示无放回。  
第二个参数num,随机取几个。  
第三个参数是种子seed，为调试方便固定一组随机值使用。    

-  RDD.scala，WordCount.scala，ErrorTopN.scala，FluxAvg.scala         
常用RDD算子的scala使用，简单常用统计练手。   
如ErrorTopN.scala：  
1.统计Nginx日志中，出现的TopN客户端IP，有可能是恶意攻击IP。  
2.统计Nginx日志中，用户访问TopN的Url，热卖商品等。    

### performance_optimize包   
生产中避免数据倾斜、性能调优的的java、scala编码使用总结。  
1.java.com.ww.rdd.performance_optimize包下java实践。  
2.scala.com.ww.rdd.performance_optimize包下scala实践。    

-  Agg2PC.java，Agg2PCs.scala   
数据倾斜的解决方案 之 两阶段聚合(局部聚合+全局聚合)。   
适用场景：对RDD执行reduceByKey等聚合类shuffle算子或者在Spark SQL中使用group by语句进行分组聚合时。   
实现原理：将原本相同的key通过附加随机前缀的方式，变成多个不同的key，
就可以让原本被一个task处理的数据分散到多个task上去做局部聚合，进而解决单个task处理数据量过多的问题。   
接着去除掉随机前缀，再次进行全局聚合，就可以得到最终的结果。   

-  BroadcastMapJoin.java，BroadcastMapJoins.scala   
数据倾斜的解决方案 之 将join使用broadcast+map替代实现。   
适用场景：在对RDD使用join类操作，或者是在Spark SQL中使用join语句时，
并且join操作中的一个RDD或表的数据量比较小（比如几百M或者一两G）。    
实现原理：普通的join是会走shuffle过程的，而一旦shuffle，
就相当于会将相同key的数据拉取到一个shuffle read task中再进行join，此时就是reduce join。   
但是如果一个RDD比较小，则可以采用broadcast小RDD全量数据+map算子来实现与join同样的效果，也就是map join。   
将较小RDD中的数据直接通过collect算子拉取到Driver端的内存中来，然后对其创建一个Broadcast变量。  
接着对另外一个RDD执行map类算子，在算子函数内，从Broadcast变量中获取较小RDD的全量数据，
与当前RDD的每一条数据按照连接key进行比对，如果连接key相同的话，那么就将两个RDD的数据用需要的方式连接起来。   
此时不会发生shuffle操作，也就不会发生数据倾斜。   

-  BigSkewJoinBigAvg.java   
数据倾斜解决方案 之 大表join大表，转为sample找出少量倾斜key加前缀均匀join，最后union。
适用场景：两个RDD进行join的时候，如果数据量都比较大，无法采用BroadcastMapJoin解决。
此时sample一下两个RDD中的key分布情况。如果出现数据倾斜且是其中某一个RDD中的少数几个key的数据量过大，
而另一个RDD中的所有key都分布比较均匀，那么本解决方案。
如果导致倾斜的key特别多的话，比如成千上万个key都导致数据倾斜，那么这种方式也不适合。
实现原理：对于join导致的数据倾斜，如果只是某几个key导致了倾斜，可以将少数几个key分拆成独立RDD，
并附加随机前缀打散成n份去进行join，此时这几个key对应的数据就不会集中在少数几个task上，而是分散到多个task进行join了。
只需要针对少数倾斜key对应的数据进行扩容n倍，不是对全量数据进行扩容。避免占用过多内存。  

##  SparkSQL    
### SparkSQL练手项目   
scala.com.ww.rdd.statproject包下，使用RDD写了个统计项目。  
-  rdd_statproject_AccessLog.txt     
模拟互联网用户访问日志。   

-  AccesslogSchema.scala   
case class方式，根据日志记录格式定义了日志文件的schema，
对读取的每条记录映射到字段。  

-  LogAnalysis.scala  
统计1：单条记录的最大流量、最小流量和平均流量。  
统计2：出现次数TopN的http响应码列表。  
统计3：找出出现超出N次的客户端IP，有可能是恶意攻击IP。  
3个统计都是基于读取数据RDD继续计算，cache缓存，不必每次从头计算，节省计算资源。用完记得释放cache。  

###  SparkSQL正式项目   
scala.com.ww.sparksql.statproject包下，使用SparkSQL写了个统计项目，模拟对网站访问日志进行统计分析。  
数据流：网络访问日志文件(使用Python生成模拟)->SparkSQL(ETL)->SparkSQL(Rdd2DataFrame)->SparkSQL(STAT)->MySQL。    

####  【数据流--1】：数据源      
-  datasource.GenNetAccessLog.py    
因没有真实数据，简单起见，使用Python模拟生成网站访问日志OriginalData.txt。  
【使用方法】：   
python GenNetAccessLog.py [生成记录数] [生成日志的路径]   
【参数默认值】：   
生成记录数如果不填写，默认生成1000条记录。  
生成日志的路径，默认在GenNetAccessLog.py当前目录下。   
【生成日志格式】：   
记录内字段间\t分隔，记录间\n分隔
字段1：IP(ETL流程使用)    
字段2：-   
字段3：-   
字段4：时间，[DD/MMM/YYYY hh:mm:ss +800] (ETL流程使用)   
字段5："POST"或"GET"  
字段6："相对URL地址"  
字段7："HTTP1.0"   
字段8："HTTP状态码"   
字段9："消耗流量字节数"(ETL流程使用)    
字段10："用户访问的URL" (ETL流程使用)   
示例：  
108.112.152.76	-	-	[25/DEC/2018 17:29:34 +0800]	"POST	video/1004	HTTP1.0"	500	2177 "http://www.videonet.com/video/1005"   

####   【数据流--2】：ETL   
-  service.ETL.scala     
1.从【数据流--1】生成的数据源文件OriginalData.txt，抽取分析所需字段，然后ETL清洗、转换为待进行Rdd2DataFrame前的文件。  
2.python特意构造了一个常见待转换日期，写个工具类utils.DateConvertUtil.scala练手下日期转换。  
注意日期转换务必不能使用SimpleDateFormat类，线程不安全，面对Spark的多线程会出现问题，工具类中使用的是线程安全的FastDateFormat。   

####   【数据流--3】：Rdd->DataFrame   
-  service.Rdd2DataFrame.scala    
在【数据流--2】ETL输出文件基础上，使用createDataFrame方法，基于DataFrameSchemaUtil工具类，schema好dataframe到parquet文件中，待统计分析stat.scala处理。  
1.RDD转为DataFrame有2种方法：   
方法1：使用反射机制推断RDD Schema。场景：运行前知道Schema。特点：代码简洁。 
scala.com.ww.rdd.statproject.AccesslogSchema.scala使用了这种方法。  
方法2：以编程方式定义RDD Schema。场景：运行前不知道Schema。   
本工程使用方法2完成RDD->DataFrame的Schema映射，编写schema映射在utils.DataFrameSchemaUtil中。  
2.数据源是IP，想区分为地市统计，github上找到一个此类中评星最高的项目ggstar，研读其说明后，编写工具类utils.Ip2CityUtil.scala，resource录入其2个映射表文件完成IP->城市的转换。  
3.输出结果文件使用了Parquet格式，生产环境推荐使用。因为Parquet采用列式存储，
避免读写不需要的数据，且具有极好的压缩比，节约内存和磁盘存储空间，降低IO，所以可提升读写性能和降低GC可能性。     

####   【数据流--4】：Stat   
基于【数据流--3】的Rdd2DataFrame.scala schema好的数据，使用SparkSQL实现3个统计，调用InsertMySQLDao将统计结果批量录入MySql。   
1.使用了SparkSQL的一些常用统计API。输出3个统计结果：
统计结果1：某日URL点击TopN视频统计。  
统计结果2：某某日分城市维度，URL点击TopN视频统计。  
统计结果3：某日访问流量TopN视频统计。   
2.将3个统计都使用的数据处理好后cache，这样不用每次都从头计算，生产环境常用性能提升技巧。用户记得释放cache：unpersist(true)。    
3.在内存hold住的前提下，使用foreachPartition,一次处理一个partition的所有数据，而不是foreach逐条数据处理，生产环境常用。  
(1)如果使用foreach函数来将RDD中所有数据写MySQL，就会一条数据一条数据地写，
每次函数调用就会创建一个数据库连接，此时就势必会频繁地创建和销毁数据库连接，性能低下。     
(2)生产环境中，可用内存大于一个partition数据量的前提下，使用foreachPartitions算子一次性处理一个partition的数据，
那么对于每个partition，只要创建一个数据库连接即可，然后批量插入再commit，提升入库性能。       
性能测试过，对于1万条左右的数据量写MySQL，性能约提升30%以上。   

####   【数据流--5】：Stat结果入MySQL   
-  dao.OperateMySQLDao.scala   
1.调用工具类MySQLUtil连接和释放MySQL。  
2.deleteData方法：重新插入某日数据前，delete某日数据。  
3.insertDayVideoTopN方法：批量插入【数据流--4】Stat的统计结果1到day_video_topn_stat表中。   
4.insertVideoCityTopN方法：批量插入【数据流--4】Stat的统计结果2到day_video_city_topn_stat表中。
5.insertDayVideoTrafficsTopN：批量插入【数据流--4】Stat的统计结果3到day_video_traffics_topn_stat表中。
注：  
1.utils.MySQLUtil.scala封装连接和释放MySQL的connection操作。  
2.model包中定义了3张表的数据库表结构。   

##  SparkStreaming    
scala.com.ww.sparkstreaming包下，对SparkStreaming特殊重要算子逐一演练。  
-   SocketTextStream.scala   
SparkStreaming入门wordcount，主要是在原理学习基础上对接NetCat发送socket流式消息实践体会。  
(1)注意sparkstreaming至少要开启2个线程，1个线程(receiver-task)接收数据，另外N个线程处理数据。  
(2)receiver-task接收流式数据后，每隔Block interval(默认200毫秒)生成1个block，存储到executor的内存中，
为保证高可用，还会有副本。注：有M个block就有M个task的并行度。  
(3)StreamingContext每隔Batch inverval(入参Seconds秒)时间间隔，blocks组成一个RDD。  

-  UpdateStateByKey.scala   
实践SocketTextStream的时候发现，reduceByKey只能在一个Batch inverval(入参Seconds秒)时间间隔内统计，
但生产中更多的统计是在更长时间间隔内的累计统计。此时，可以使用UpdateStateByKey这个按key累计计数的算子。  
使用updateStateByKey一定要设置一个checkpoint的目录。因为计算结果有中间状态，中间状态是长时间历史累计的，
放在内存里不可靠，需要存储在磁盘上，避免task出问题时，前面的累计值可以从checkpoint获取到。  
UpdateStateByKey的入参是updateFunc: (Seq\[Int], Option\[S]) => Option\[S]，是一个匿名函数。   
(Seq\[Int], Option\[S])是入参，Option\[S]是返回值。   
(1)入参Seq\[Int]：Seq代表的是一个集合，int代表的是(key,value)中的value的数据类型。例如("ww",{1,1,1,1}),Seq\[Int]是指的{1,1,1,1}。  
(2)入参Option\[S]：S代表的是累计值中间状态State的数据类型，S对于本wordcount代码来说，是int类型。中间状态存储的是单词累计出现的次数，如"ww"的累计状态 -> 4。  
(3)返回值Option\[S]，与累计的中间状态数据类型是一样的。  

-  MapWithState.scala    
MapWithState也是累计统计算子，推荐使用。因为与updateStateByKey方法相比，使用mapWithState方法能够得到6倍的低延迟同时维护的key状态数量要多10倍。  
同样，使用MapWithState一定要设置一个checkpoint的目录。  
mapWithState的入参：累计函数mappingFunc，函数有3个入参：   
入参1：word: String  代表的是key。  
入参2：one: Option\[Int] 代表的是value。  
入参3：state: State\[Int] 代表的是历史状态，也就是上次的累计结果。   

-  ReduceByKeyAndWindow.scala    
滑动窗口统计：Seconds参数设置每N秒生成一个RDD，滑动窗口Window操作可以实现，每隔a\*N秒 实时统计 前b*N秒 的统计计数情况。  
生产环境常用，reduceByKeyAndWindow的3个入参：  
入参1：ReduceByKey方法。  
入参2：每次统计前面多少时间粒度间隔(必须是Seconds参数的倍数)的数据。    
入参3：每次统计的时间间隔，也就是滑动窗口的宽度(必须是Seconds参数的倍数)。   

-  Transform.scala    
transform操作，应用在DStream上时，可以用于执行任意的RDD到RDD的转换操作，通常用于实现DStream API中所没有提供的操作。  
例如：DStream API中，没有提供将一个DStream中的每个batch，与一个特定的RDD进行join的操作，可以使用transform操作来实现该功能。  
使用transform实现join代码说明：
(1)定义filterCharactor：想要流式计算不对这些字符做统计计数，类似业务上的黑名单过滤。  
(2)transform内部封装处理：leftOuterJoin+filter，输出格式格式是 String,(int,option\[boolean])。  
(3)transform内部封装处理：然后map将格式转换为(word:String, count:int)的格式，reduceByKey计算得到结果。  

-  foreachrdd包：  
使用foreachRDD，每次批量获取一个partition的数据处理，而不是逐条处理，减少IO提升性能。  
1.ForEachRDDWordCount.sql：在MySQL里创建一张统计结果表。   
2.MysqlPool.scala：写了个简单版的数据库连接池。    
(1)releaseConn方法：用操作完数据库后，释放Connection，把Connection数据库连接push回连接池。  
(2)loadDBDriver方法：数据库连接池已没有空闲连接，并且当前已经产生的连接数<数据库允许的最大连接数，才允许加载驱动新建数据库连接；
数据库连接池已没有空闲连接，并且当前已经产生的连接数>=数据库允许的最大连接数，拒绝加载数据库驱动连接，打印提示，等待2秒重新尝试。   
(3)getJdbcConn方法：虑多线程，使用同步代码块AnyRef.synchronized；每次批量生成newConnections个数据库连接；pool.poll()。  
3.ForEachRDDMySql.scala：对于统计结果入MySQL等数据库场景，foreachRDD每次批量获取一个partition的数据建立一个数据库连接批量入库，减少了数据库连接的创建，生产中常用。  

-  KafkaIntegration.scala      
SparkStreaming2.4.0与Kafka2.1.1简单集成：  
1.kafkaParams：设置Kafka相关参数。   
2.KafkaUtils.createDirectStream：Spark2.2版本之后，只保留了pull方式拉取Kafka数据的方式，因为这种方式更优，不会因为SparkStreaming处理task性能不足或异常导致数据积压在Receiver里。  
KafkaUtils.createDirectStream返回值是Map，key是Kafka的offset，value是消费到的内容数据。  
从KafkaUtils.createDirectStream返回值取value，获取到Kafka消费数据的内容，然后做统计。  
3.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")：序列化要使用Kryo，默认的Java序列化报错。   
4.Driver高可用：   
(1)定义functionToCreateContext方法，将streamingContext初始化、设置checkpoint等封装在里面。  
(2)StreamingContext.getOrCreate(checkpointDir, functionToCreateContext _)：启动时，Driver检查checkpoint目录，如果没有数据则创建，如果有数据加载。  
使用上述方法保证了Driver的高可用，生产环境里都是这样用的。  
遗留问题：  
代码未考虑offset的管理问题，生产环境是自己管理offset的，业务处理完成后再提交zookeeper标记已消费。  
实在太忙了，有空再完善这部分代码。  




