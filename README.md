# 项目背景
---------
公司核心的实时业务用的是spark streaming2.3.0+kafka1.3的流式技术来开发的。在这里我把它做成了一个骨架项目并开源出来，希望后来的朋友可以借阅和参考，尽量少走些弯路。

下面是使用过程中记录的一些心得和博客，感兴趣的朋友可以了解下：

- [如何管理Spark Streaming消费Kafka的偏移量（一）](https://www.jianshu.com/p/9fc343879bbc)
- [如何管理Spark Streaming消费Kafka的偏移量（二）](https://www.jianshu.com/p/9bb983f86415)
- [如何管理Spark Streaming消费Kafka的偏移量（三）](https://www.jianshu.com/p/bf422de60e8b)
- [Spark Streaming 重启后Kafka数据堆积调优](https://www.jianshu.com/p/63f52743ae77)
- [spark stream冷启动处理kafka中积压的数据](https://www.jianshu.com/p/8f13735d40bd)
- [SparkStreaming如何优雅的停止服务](https://www.jianshu.com/p/e92bd93fa1bc)
- [Spark Streaming优雅的关闭策略优化](https://www.jianshu.com/p/2a7ec7e57130)

# 项目简介
-----------
该项目提供了一个在使用spark streaming2.3+kafka1.3的版本集成时，手动存储偏移量到zookeeper中，因为自带的checkpoint弊端太多，不利于项目升级发布，并修复了一些遇到的bug，例子中的代码已经在我们生产环境运行，所以大家可以参考一下。

# 主要功能
------------
1. 提供了快速使用 spark streaming + kafka 开发流式程序的骨架，示例中的代码大部分都加上了详细的注释
2. 提供了手动管理kafka的offset存储到zookeeper的方法，并解决了一些bug，如kafka扩容分区，重启实时流不识别新增分区的问题。
3. 提供了比较简单和优雅的关闭spark streaming流式程序功能

# 个人博客：
--------
简书：https://www.jianshu.com/u/41307d187d27