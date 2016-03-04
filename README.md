##Chuye.Kafka
Chuye.Kafka 是 Kafka API 的 .Net 实现，基于 0.9 版本。

* 提供几乎匹配协议语义的数据结构，便于理解和完全底层的使用；
* 提供更加透明的轻度封装的 Producer 和 Consumer，使用起来接近于无状态协议的体验；

##使用
NuGet package 
PM> Install-Package Chuye.Kafka

##性能
单虚拟机 Producer immediate 模式下适量合并消息轻松达到10万条+/秒，见脚本部分

##反馈
在使用中有任何问题，欢迎交流 jusfr.v@gmail.com

##计划

* 消息压缩
* Zookeeper API

##问题
* 如何配置：见源码中的 App.config，也可以像下文示例一样构造 KafkaConfigurationSection 对象；
* 如何连接到 Broker 集群： Kafka 通过 TopicMeatadata 进行自描述，使用 Connection 的地方以 Router 替换即可；
* 消费者分组： Kafka 语义下的发布订阅模式——同组消费者被分配到 Topic 的不同 Partition 及 Reblance 逻辑严重依赖 Zookeepr，本质上是客户端编程实现的，其 high-level-api 消费者对 Offset 进行了定时/条件提交，Chuye.Kafka 并没有依样实现，而是以 Conumser 的轻量封装替代，用户可以在各场景下实现更精细的控制；
  分组带来的读写时的 Partition 显示指定问题，Kafka 文档里也没有给出很好的方案，Chuye.Kafka 实现了基于配置的策略，使客户端可以固定读写某分组而不是分区算法；
* Topic 在哪里管理：这涉及到 Zookeepr 的操作，列在计划之中了；  
* Connection 对象的管理：它持有了 Socket 池与 Buffer 池以解决 Socket.Connect() 与 Byte[] 分配造成的性能问题，需要与应用程序的生命周期相同；

##示例
来自 $/doc/Chuye.Kafka-high-level-api.linq，移除 .Dump() 语法则是常规代码片段

```c
//TopicMetadata
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    //var demoTopics = new String[] { }; // Empty array to grab all metadata
    var demoTopics = new String[] { "demoTopic" };
    var connection = new Router(section);
    connection.TopicMetadata(demoTopics).Dump("Metadata");
}

//Produce
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    const String demoTopic = "demoTopic";
    var producer = new Producer(new Router(section));
    producer.Strategy = AcknowlegeStrategy.Written;
    const Int32 count = 10;
    for (int i = 0; i < count; i++) {
        var message = String.Concat(Guid.NewGuid().ToString("n"), "#", i);
        producer.Post(demoTopic, message);
    }
}

//Fetch
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    section.Buffer.ResponseBufferSize = 10 * 1024;
    const String demoTopic = "demoTopic";
    var consumer = new Consumer(new Router(section));
    consumer.Fetch(demoTopic, 0).Dump("Fetch");
}

//Offset
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    const String demoTopic = "demoTopic";
    var consumer = new Consumer(new Router(section));
    var earliest = consumer.Offset(demoTopic, OffsetTimeOption.Earliest);
    var latest = consumer.Offset(demoTopic, OffsetTimeOption.Latest);
    new { earliest, latest }.Dump();
}

//OffsetCommit
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    const String demoTopic = "demoTopic";
    const String demoConsumerGroup = "demoConsumerGroup";
    var consumer = new Consumer(new Router(section));
    consumer.OffsetCommit(demoTopic, demoConsumerGroup, 6);
}

//OffsetFetch
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    const String demoTopic = "demoTopic";
    const String demoConsumerGroup = "demoConsumerGroup";
    var consumer = new Consumer(new Router(section));
    consumer.OffsetFetch(demoTopic, demoConsumerGroup).Dump("OffsetFetch");
}

//Producer benchmark
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    section.Buffer.RequestBufferSize = 1024;
    const String demoTopic = "demoTopic";
    var connection = new Router(section);
    var producer = new Producer(connection);
    producer.Strategy = AcknowlegeStrategy.Immediate;
    var stopwatch = Stopwatch.StartNew();
    const Int32 count = 10000;
    for (int i = 0; i < count; i++) {
        var message = String.Concat(Guid.NewGuid().ToString("n"), "#", i);
        producer.Post(demoTopic, message);
    }
    stopwatch.Stop();
    Connection.Statistic.Dump();
    Console.WriteLine("Handle {0} messages in {1}, {2:f3} /sec.",
        count, stopwatch.Elapsed,
        count / stopwatch.Elapsed.TotalSeconds);
    Console.WriteLine("Bytes sended {0:f2} MB, {1:f2} MB/sec.",
        Connection.Statistic.ByteSended / 1048576.0,
        Connection.Statistic.ByteSended / 1048576.0 / stopwatch.Elapsed.TotalSeconds);
}
```
