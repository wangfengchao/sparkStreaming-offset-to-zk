package com.server


import com.utils.{CommonUtils, GraceCloseUtils, KafkaOffsetManager}
import kafka.api.OffsetRequest
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.LogManager
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming优雅关闭服务策略 和 冷启动时Kafka数据堆积优化
  * Created by fc.w on 2018/05/30
  */
object StreamingOffsetLauncher {

  lazy val log = LogManager.getLogger("StreamingOffsetLauncher")

  /**
    * 程序入口
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val ssc = StreamingContext.getOrCreate(CommonUtils.checkpoint, functionToCreateContext)

    ssc.start()

    // 方式一： 通过Http方式优雅的关闭策略
    GraceCloseUtils.daemonHttpServer(5555,ssc)
    // 方式二： 通过扫描HDFS文件来优雅的关闭
    // GraceCloseUtils.stopByMarkFile(ssc)

    //等待任务终止
    ssc.awaitTermination()
  }

  /**
    * 主逻辑
    * @return
    */
  def functionToCreateContext(): StreamingContext = {
    val conf = new SparkConf().setAppName("streaming_offset_to_zk_app")
    if (CommonUtils.isLocal) conf.setMaster("local[1]") // local模式

    /* 启动优雅关闭服务 */
    conf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    /* Spark Streaming 重启后Kafka数据堆积调优 */
    conf.set("spark.streaming.backpressure.enabled", "true") // 激活反压功能
    conf.set("spark.streaming.backpressure.initialRate", "5000") // 启动反压功能后，读取的最大数据量
    conf.set("spark.streaming.kafka.maxRatePerPartition", "2000") // 设置每秒每个分区最大获取日志数，控制处理数据量，保证数据均匀处理。

    var kafkaParams = Map[String, String]("bootstrap.servers" -> "10.108.4.203:9092,10.108.4.204:9092,10.108.4.205:9092") // 创建一个kafkaParams
    if (CommonUtils.firstReadLastest) kafkaParams += ("auto.offset.reset" -> OffsetRequest.LargestTimeString) // 从最新的开始消费
    // 创建zkClient注意最后一个参数最好是ZKStringSerializer类型的，不然写进去zk里面的偏移量是乱码
    val zkClient = new ZkClient("10.108.4.203:2181,10.108.4.204:2181,10.108.4.205:2181", 30000, 30000, ZKStringSerializer)
    val zkOffsetPath = CommonUtils.zkOffsetPath
    val topicsSet = CommonUtils.topicSet

    val ssc = new StreamingContext(conf, Seconds(10))
    val rdds: InputDStream[(String, String)] = createKafkaStream(ssc, kafkaParams,zkClient,zkOffsetPath, topicsSet)
    rdds.foreachRDD(rdd => {
      // 只处理有数据的rdd，没有数据的直接跳过
      if(!rdd.isEmpty()){

        // 迭代分区，里面的代码是运行在executor上面
        rdd.foreachPartition(partitions => {

          //如果没有使用广播变量，连接资源就在这个地方初始化
          //比如数据库连接，hbase，elasticsearch，solr，等等
          partitions.foreach(msg => {
            log.info("读取的数据：" + msg)
          })
        })
      }

      // 更新每个批次的偏移量到zk中，注意这段代码是在driver上执行的
      KafkaOffsetManager.saveOffsets(zkClient,zkOffsetPath,rdd)
    })

    ssc
  }

  /**
    * 获取Kafka数据偏移量
    * @param ssc StreamingContext
    * @param kafkaParams  配置kafka的参数
    * @param zkClient zk连接的client
    * @param zkOffsetPath zk里面偏移量的路径
    * @param topics 需要处理的topic
    * @return InputDStream[(String, String)] 返回输入流
    */
  def createKafkaStream(ssc: StreamingContext,
                        kafkaParams: Map[String, String],
                        zkClient: ZkClient,
                        zkOffsetPath: String,
                        topics: Set[String]): InputDStream[(String, String)] = {
    // 目前仅支持一个topic的偏移量处理，读取zk里面偏移量字符串
    val zkOffsetData = KafkaOffsetManager.readOffsets(zkClient, zkOffsetPath, topics.last)
    val kafkaStream = zkOffsetData match {
      case None => //如果从zk里面没有读到偏移量，就说明是系统第一次启动
        log.info("系统第一次启动，没有读取到偏移量，默认就最新的offset开始消费")
        // 使用最新的偏移量创建DirectStream
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

      case Some(lastStopOffset) =>
        log.info("从zk中读取到偏移量，从上次的偏移量开始消费数据......")
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        // 使用上次停止时候的偏移量创建DirectStream
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, lastStopOffset, messageHandler)

    }

    kafkaStream // 返回创建的kafkaStream
  }

}
