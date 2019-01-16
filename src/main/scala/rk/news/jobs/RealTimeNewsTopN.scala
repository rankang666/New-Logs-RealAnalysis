package rk.news.jobs

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import rk.news.conf.Constants
import rk.news.utils.{DBCPUtils, JedisUtil}

/**
  * @Author rk
  * @Date 2018/12/1 15:50
  * @Description:
  **/
object RealTimeNewsTopN {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.spark_project").setLevel(Level.WARN)

    if (args == null || args.length < 5) {
      println(
        """ Parameter Errors! Usage: <batchInterval>  <topics>  <checkpoint>  <topn>  <table>
          |batchInterval:   提交sparkjob的间隔时间
          |topics       :   要消费的kafka的topic的集合，使用,隔开
          |checkpoint   :   checkPoint
          |topn         :   topn
          |table        :   table
        """.stripMargin)
      System.exit(-1)
    }
    val Array(batchInterval, topics, checkpoint, topn, table) = args

    val session = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[2]")
      .getOrCreate()

    //real time 统计的需要streamingContext
    val sc = session.sparkContext
    val ssc = new StreamingContext(sc, Seconds(batchInterval.toLong))
    val sqlContext = session.sqlContext
    ssc.checkpoint(checkpoint)

    //读取kafka的数据
    val meassages: DStream[String] = createStream(ssc, topics)
    meassages.print()

    /**
      * 实时分析前20名流量最高的新闻话题
      * 实际上求的每一个新闻话题的wordcount，然后排序，求最高的前20
      * 实时分析前20名的流量最高，就要计算截止到目前为止的数据流量，不是单一批次
      *   updataStateByKey
      *   启动一个checkpoint的位置，用来存储截止到目前为止之前的数据
      *   新闻话题就是log中的第三列: searchname
      *
      */
    val smPairDStream: DStream[(String, Int)] = meassages.map { case msg => {
      val fields = msg.split("\t")
      val searchname = fields(2)
      (searchname, 1)
    }
    }
    val batchTopics2Count: DStream[(String, Int)] = smPairDStream.reduceByKey(_+_)

    /**
      * MEMORY_ONLY：  效率最快，最消耗资源
      *         数据是没有经过序列化保存在内存中，容易造成OOM，gc的频率和object的个数成正比 不建议使用（100条以内可以考虑）
      * MEMORY_ONLY_SER：  针对上述的优化，数据经过序列化存储，一个partition的数据就只有一个object（默认3M以上进入老年代）
      *          这里的性能消耗在于：序列化和反序列化 kryo
      * MERORY_AND_DISK (不建议)
      * MEMORY_AND_DISK_SER（建议）
      * DISK_ONLY（不用）
      * MEMORY_ONLY_2,MEMORY_AND_DISK_2（不建议，除非对数据的容错性要求非常高）
      * OFF_HEAP(experimental)  堆外内存： 以上数据都要占用executor的内存
      *     executor中的  spark.storge.memoeyFraction:  0.6(持久化的数据，占到了executor内存的60%)
      *                   spark.shuffle.memoeyFraction: 0.2(shuffle,占到了executor内存的20%)
      *
      * alluxio(tachyon --> 基于内存的hdfs版本)
      */
    batchTopics2Count.persist(StorageLevel.MEMORY_ONLY)

    /**
      * 将话题数据插入到redis中
      *     实时或准实时统计中，不建议将数据落地到mysql中，高频操作，mysql服务承载压力有限，如果可以到达大概每秒10w条，可以考虑
      *     所以建议，可以将结果落地到hbase, redis, es, ignite等
      *
      */
    batchTopics2Count.foreachRDD(rdd => {
      if(! rdd.isEmpty()){
        rdd.foreachPartition(partition =>{
          if(! partition.isEmpty){
            val jedis = JedisUtil.getJedis
            partition.foreach{case (topic, topicNum) =>{
              jedis.sadd(Constants.NEW_TOPICS, topic)
            }}
            JedisUtil.close(jedis)
          }
        })
      }
    })




    /**
      * 截止到目前的所有话题的浏览量
      *   总的数据量 = 当前批次的数据量 + 之前的数据量
      *   之前的数据:
      *     A ---> 10     B ---> 11
      *    新来了一批数据:
      *     C ---> 2      B ---> 1
      *     updateFunc: (Seq[V], Option[S])
      *     a left join b
      *     a表所有的都显示，b表中对应不上的显示null
      *
      */
    val usbDStream: DStream[(String, Int)] = batchTopics2Count.updateStateByKey((current: Seq[Int], history: Option[Int]) => {
      val sum = current.sum + history.getOrElse(0)
      Option[Int](sum)
    })
    //    usbDStream.print()

    //前20名  --->  发现streaming无法进行排序，要么是用core，要么是用sql进行排序
    usbDStream.foreachRDD(rdd =>{
      if(! rdd.isEmpty()){
        import sqlContext.implicits._
        val smDF: DataFrame = rdd.toDF("searchname","times")
        smDF.createOrReplaceTempView("searchname_times_tmp")
        val retDF: DataFrame = sqlContext.sql(s"select searchname topic, times from searchname_times_tmp order by times desc limit ${topn} ")
        //        retDF.show()
        //        将结果落地到mysql

        val properties = new Properties
        properties.put("user",DBCPUtils.username)
        properties.put("password",DBCPUtils.password)
        retDF.write.mode(SaveMode.Overwrite).jdbc(
          DBCPUtils.url, table, properties
        )
      }
    })



    ssc.start()
    ssc.awaitTermination()


    session.stop()


  }


  /**
    * 读取kafka中的数据
    *
    * @param ssc
    * @param topics
    * @return
    *
    * receiver V.S. Direct
    * 在流计算中有三种语义需要处理：
    * at least once:  至少一次
    * 一条记录被重复消费
    * at most once:   最多一次
    * 一条记录由于失败的原因没有被成功消费
    * exactly once:  恰好一次
    *
    * 原本kafka   --> zk
    * sparkstreming
    * 如何解决receiver带来的问题：  开启checkpoint,开启wal
    *
    * 遵循的原则:  移动计算不移动数据
    * 数据的本地性:
    * 数据和计算它的代码之间的距离
    * process:  进程级别(数据和代码在同一个进程)
    * node: 数据和代码在同一节点的不同进程中
    * rack: 不同节点，机架
    *
    *
    */
  def createStream(ssc: StreamingContext, topics: String): DStream[String] = {
    /**
      * LocationStrategy: 主要指定的数据加载的策略
      * 主要是读取数据性能问题的考虑，选择不同的数据本地性级别来加载数据。
      * 核心原因在于,新版的api要预先fentch一批数据放到buffer
      * PreferBrokers:  如果brokers和spark作业的executor在相同的节点的时候，可以选择这种方式。
      * PreferConsistent: 最常用的一种数据加载策略，为每一个executor，只会去分发不同partition中的数据。
      * PreferFixed:  固定的方式，在某些host上加载不同的partition,
      * PreferFixed(hostMap:collection.Map[TopicPartition,String])
      * Map[TopicPartition,String](
      * (new TopicPartition("news-logs",0) -> "hadoop03"),
      * (new TopicPartition("news-logs",1) -> "hadoop01"),
      * (new TopicPartition("news-logs",2) -> "hadoop02")
      * )
      * Topic:news-logs PartitionCount:3        ReplicationFactor:2     Configs:
      * Topic: news-logs        Partition: 0    Leader: 3       Replicas: 3,1   Isr: 1,3
      * Topic: news-logs        Partition: 1    Leader: 1       Replicas: 1,2   Isr: 1,2
      * Topic: news-logs        Partition: 2    Leader: 2       Replicas: 2,3   Isr: 2,3
      *
      * ConsumerStrategy: 主要就是用来指定kafka的相关配置信息的。
      * "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      * "key.deserializer" -> classOf[StringDeserializer],
      * "value.deserializer" -> classOf[StringDeserializer],
      * "group.id" -> "use_a_separate_group_id_for_each_stream",
      * "auto.offset.reset" -> "latest",
      * "enable.auto.commit" -> (false: java.lang.Boolean)
      *
      */

    val kafkaParams = Map(
      "bootstrap.servers" -> "hadoop01:9092,hadoop02:9092,hadoop03:9092",
//      "bootstrap.servers" -> "hadoop:19092,hadoop:29092,hadoop:39092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "news_logs_topgroup"
    )


    val kafkaStream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream(
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe(topics.split(","), kafkaParams)
      )

    val messages: DStream[String] = kafkaStream.map(record => record.value())
    messages

  }


}
