package rk.news.jobs

import java.sql.DriverManager
import java.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import rk.news.dao.impl.DefaultHourTopicsDaoImpl
import rk.news.entity.HourTopics
import rk.news.utils.DBCPUtils

/**
  * @Author rk
  * @Date 2018/12/1 15:50
  * @Description:
  *              版本一写入到数据库的时候有乱码和重复等问题，所以版本二进行优化
  **/
object RealTimeNewsHourTopN2 {
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
      *统计各个小时的浏览量
      * 00:00:00	2982199073774412	[360安全卫士]	8	3	download.it.com.cn/softweb/software/firewall/antivirus/20067/17938.html
      */
    val hourPairDStream: DStream[(String, Int)] = meassages.map { case msg => {
      val fields = msg.split("\t")
      if (fields == null || fields.length != 6){
        ("default", 0)
      }else{
        val datetime = fields(0).trim
        var hour = datetime.substring(0,datetime.indexOf(":"))
        if(hour.length > 2){
          hour = hour.substring(hour.length-2,hour.length)
        }
        (hour, 1)

      }
    }}
    val hourTopics2Count: DStream[(String, Int)] = hourPairDStream.reduceByKey(_+_)

    val usbDStream: DStream[(String, Int)] = hourTopics2Count.updateStateByKey((current: Seq[Int], history: Option[Int]) => {
      val sum = current.sum + history.getOrElse(0)
      Option[Int](sum)
    })
//    usbDStream.print()

    //使用sparkStreaming直接将结果落地到mysql数据库
    usbDStream.foreachRDD(rdd =>{
      if(! rdd.isEmpty()){
        rdd.foreachPartition(partition => {
          if(!partition.isEmpty){
            val hourTopicDao = new DefaultHourTopicsDaoImpl
            val hts = new util.ArrayList[HourTopics]()
            partition.foreach{ case (hour, count) => {
              val ht = new HourTopics
              ht.setHour(hour)
              ht.setTimes(count)
              hts.add(ht)

            }}
           hourTopicDao.insertBatch(hts);



          }
        })
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
      "group.id"  -> "news_logs_hourgroup"
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
