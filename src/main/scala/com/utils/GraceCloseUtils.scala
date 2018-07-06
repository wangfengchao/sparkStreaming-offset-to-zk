package com.utils

import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.LogManager
import org.apache.spark.streaming.StreamingContext
import org.spark_project.jetty.server.handler.{AbstractHandler, ContextHandler}
import org.spark_project.jetty.server.{Request, Server}

/**
  * Streaming 两种优雅的停止策略：
  * 1. 通过http服务
  * 2. 通过扫描hdfs文件
  * Created by fc.w on 2018/05/30
  */
object GraceCloseUtils {

  lazy val log = LogManager.getLogger("GraceCloseUtils")

  /**
    * 1. HTTP方式
    * 负责启动守护的jetty服务
    * @param port 对外暴露的端口号
    * @param ssc Stream上下文
    */
  def daemonHttpServer(port:Int, ssc: StreamingContext) = {
    val server = new Server(port)
    val context = new ContextHandler()
    context.setContextPath("/close")
    context.setHandler(new CloseStreamHandler(ssc))
    server.setHandler(context)
    server.start()
  }

  /**
    * 负责接受http请求来优雅的关闭流
    * @param ssc Stream上下文
    */
  class CloseStreamHandler(ssc:StreamingContext) extends AbstractHandler {
    override def handle(s: String, baseRequest: Request, req: HttpServletRequest, response: HttpServletResponse): Unit = {
      log.warn("开始关闭......")
      // 优雅的关闭
      ssc.stop(true, true)
      response.setContentType("text/html; charset=utf-8")
      response.setStatus(HttpServletResponse.SC_OK)
      val out = response.getWriter
      out.println("Close Success")
      baseRequest.setHandled(true)
      log.warn("关闭成功.....")
    }

  }


  /**
    * 2. HDFS文件检测方式
    * 通过一个消息文件来定时触发是否需要关闭流程序
    * @param ssc StreamingContext
    */
  def stopByMarkFile(ssc:StreamingContext): Unit = {
    val intervalMills = 10 * 1000 // 每隔10秒扫描一次消息是否存在
    var isStop = false
    val hdfsFilePath = "/spark/streaming/stop" // 判断消息文件是否存在
    while (!isStop) {
      isStop = ssc.awaitTerminationOrTimeout(intervalMills)
      if (! isStop && isExistsMarkFile(hdfsFilePath)) {
        log.warn("2秒后开始关闭sparstreaming程序.....")
        Thread.sleep(2000)
        ssc.stop(true, true)
      }
    }
  }

  /**
    * 判断是否存在mark file
    * @param hdfsFilePath mark文件的路径
    * @return
    */
  def isExistsMarkFile(hdfsFilePath: String): Boolean = {
    val conf = new Configuration()
    val path = new Path(hdfsFilePath)
    val fs = path.getFileSystem(conf)
    fs.exists(path)
  }

}
