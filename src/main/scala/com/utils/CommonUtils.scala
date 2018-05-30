package com.utils

import com.typesafe.config.ConfigFactory

/**
  * 读取配置文件
  * Created by fc.w on 2018/05/29
  */
object CommonUtils {

  val config = ConfigFactory.load()

  val env  = config.getString("env")
  val isLocal = if (env == "dev") true else false // 是否使用本地模式
  val checkpoint = config.getString("checkpoint")
  val firstReadLastest = true // 第一次启动是否从最新位置开始消费
  val topicSet = config.getString("kafka_topics").split(",").toSet
  val zkOffsetPath =config.getString("zkOffsetPath") // zk的路径
  val kafkaServers = config.getString("kafkaServers") // Kafka服务地址
  val zkServer = config.getString("zkServer") // ZK服务地址

}
