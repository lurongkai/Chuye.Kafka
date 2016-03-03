<Query Kind="Statements">
  <Reference>C:\Users\Administrator\Documents\Visual Studio 2015\Projects\Chuye.Kafka\release\Chuye.Kafka\Chuye.Kafka.dll</Reference>
  <Reference>&lt;RuntimeDirectory&gt;\System.Web.dll</Reference>
  <Namespace>Chuye.Kafka</Namespace>
  <Namespace>Chuye.Kafka.Protocol</Namespace>
  <Namespace>Chuye.Kafka.Protocol.Implement</Namespace>
  <Namespace>Chuye.Kafka.Protocol.Implement.Management</Namespace>
  <Namespace>System.Web</Namespace>
</Query>

//Metadata
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    //var demoTopics = new String[] { };
    var demoTopics = new String[] { "demoTopic" };
    var connection = new Router(section);
    connection.TopicMetadata(demoTopics).Dump("Metadata");
}
//Producer
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    const String demoTopic = "demoTopic";
    //const String demoConsumerGroup = "demoConsumerGroup";
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
    //const String demoConsumerGroup = "demoConsumerGroup";
    var consumer = new Consumer(new Router(section));
    consumer.Fetch(demoTopic, 0).Dump("Fetch");
}
//Offset
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    const String demoTopic = "demoTopic";
    //const String demoConsumerGroup = "demoConsumerGroup";
    var consumer = new Consumer(new Connection(section));
    var earliest = consumer.Offset(demoTopic, OffsetTimeOption.Earliest);
    var latest = consumer.Offset(demoTopic, OffsetTimeOption.Latest);
    new { earliest, latest }.Dump();
}
//OffsetCommit
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    const String demoTopic = "demoTopic";
    const String demoConsumerGroup = "demoConsumerGroup";
    var consumer = new Consumer(new Connection(section));
    consumer.OffsetCommit(demoTopic, demoConsumerGroup, 6);
}
//OffsetFetch
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    const String demoTopic = "demoTopic";
    const String demoConsumerGroup = "demoConsumerGroup";
    var consumer = new Consumer(new Connection(section));
    consumer.OffsetFetch(demoTopic, demoConsumerGroup).Dump("OffsetFetch");
}

//Producer benchmark
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    section.Buffer.RequestBufferSize = 1024;
    const String demoTopic = "demoTopic";
    //const String demoConsumerGroup = "demoConsumerGroup";
    var connection = new Router(section);
    connection.TopicMetadata(demoTopic);
    var producer = new Producer(connection);
    producer.Strategy = AcknowlegeStrategy.Immediate;
    var stopwatch = Stopwatch.StartNew();
    const Int32 count = 10000;
    for (int i = 0; i < count; i++) {
        var message = String.Concat(Guid.NewGuid().ToString("n"), "#", i);
        producer.Post(demoTopic, message);
    }
    stopwatch.Stop();
    connection.TopicMetadata(demoTopic);
    Connection.Statistic.Dump();
    Console.WriteLine("Handle {0} messages in {1}, {2:f3} /sec.",
        count, stopwatch.Elapsed,
        count / stopwatch.Elapsed.TotalSeconds);
    Console.WriteLine("Bytes sended {0:f2} MB, {1:f2} MB/spc.",
        Connection.Statistic.ByteSended / 1048576.0,
        Connection.Statistic.ByteSended / 1048576.0 / stopwatch.Elapsed.TotalSeconds);
}

//Producer benchmark
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    section.Buffer.RequestBufferSize = 10240;
    const String demoTopic = "demoTopic";
    var connection = new Router(section);
    connection.TopicMetadata(demoTopic);
    var producer = new Producer(connection);
    producer.Strategy = AcknowlegeStrategy.Immediate;

    var stopwatch = Stopwatch.StartNew();
    const Int32 count = 10000;
    const Int32 range = 10;
    var messages = new List<KeyedMessage>();
    for (int i = 0; i < count; i++) {
        messages.Clear();
        for (int j = 0; j < range; j++) {
            var message = String.Concat(Guid.NewGuid().ToString("n"), "#", i);
            messages.Add(message);
        }
        producer.Post(demoTopic, messages);
    }
    connection.TopicMetadata(demoTopic);
    stopwatch.Stop();
    Connection.Statistic.Dump();
    Console.WriteLine("Handle {0} messages in {1}, {2} /sec.",
        count * range, stopwatch.Elapsed,
        count * range / stopwatch.Elapsed.TotalSeconds);
    Console.WriteLine("Bytes sended {0:f2} MB, {1:f2} MB/spc.",
        Connection.Statistic.ByteSended / 1048576.0,
        Connection.Statistic.ByteSended / 1048576.0 / stopwatch.Elapsed.TotalSeconds);
}