using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;
using Chuye.Kafka.Protocol.Implement.Management;

namespace Chuye.Kafka {
    class Program {
        const String DemoTopic = "demo-topic";

        static void Main(string[] args) {
            Debug.Listeners.Clear();
            //Debug.Listeners.Add(new TextWriterTraceListener(Console.Out));
            //ProduceDemo();
            //ConsumerDemo();
            //SendMessag_Async();

            if (Debugger.IsAttached) {
                Console.WriteLine("Press <Enter> to exit");
                Console.ReadLine();
            }
        }

        static void SendMessag_Async() {
            const String targetTopic = "async-topic";
            var option = Option.LoadDefault();
            var producer = new Producer(option);
            producer.Strategy = AcknowlegeStrategy.Immediate;

            const Int32 count = 10;
            var stopwatch = Stopwatch.StartNew();
            for (int i = 0; i < count; i++) {
                var message = String.Concat(Guid.NewGuid().ToString("n"), "#", i);
                Console.WriteLine("Sending... {0}", message);
                producer.Post(targetTopic, message);
            }
            stopwatch.Stop();
            Console.WriteLine("Handle {0} messages in {1}, {2} /sec.",
                count, stopwatch.Elapsed, count / stopwatch.Elapsed.TotalSeconds);
        }

        #region high level api demo
        static void ConsumerDemo() {
            var option   = Option.LoadDefault();
            var consumer = new Consumer(option);
            var metadata = consumer.FetchMetadata(DemoTopic);
            var offset   = consumer.FetchOffset(DemoTopic);
            var messages = consumer.Fetch(DemoTopic, offset);

            foreach (var msg in messages) {
                Console.WriteLine(msg);
            }
        }

        static void ProduceDemo() {
            var sendingMessages = new List<KeyedMessage>();
            sendingMessages.Add("Hello kafka, demo message begin at " + DateTime.Now);
            sendingMessages.Add("Set some cloud service");
            sendingMessages.Add(new KeyedMessage("cloud", "Microsoft Azure"));
            sendingMessages.Add(new KeyedMessage("cloud", "Amazon Aws"));
            sendingMessages.Add("Set some animals");
            sendingMessages.Add(new KeyedMessage("animals", "penguin"));
            sendingMessages.Add(new KeyedMessage("animals", "bear"));
            sendingMessages.Add(new KeyedMessage("animals", "anteater"));
            sendingMessages.Add("Demo message ended at " + DateTime.Now);

            var option = Option.LoadDefault();
            var producer = new Producer(option);
            producer.Strategy = AcknowlegeStrategy.Written;
            producer.Post(DemoTopic, sendingMessages);
        }
        #endregion

        #region low level api demo
        private static void ProceedListGroups_DescribeGroups() {
            var listGroupsRequest = new ListGroupsRequest();
            var listGroupsResponse = (ListGroupsResponse)InvokeRequest(listGroupsRequest);
            var describeGroupsRequest = new DescribeGroupsRequest();
            describeGroupsRequest.GroupId = new[] { "xx" };
            var describeGroupsResponse = (DescribeGroupsResponse)InvokeRequest(describeGroupsRequest);

        }

        static void ProceedGroupCoordinator() {
            var request = new GroupCoordinatorRequest();
            //request.GroupId = String.Empty;
            request.GroupId = "100";
            var response = (GroupCoordinatorResponse)InvokeRequest(request);
        }

        static void ProceedOffsetCommit() {
            var request = new OffsetCommitRequest();
            request.ApiVersion = 2;
            request.ConsumerGroupId = "cg1";
            //request.ConsumerGroupGenerationId = 1;
            request.ConsumerId = "c1";
            request.RetentionTime = 0;
            request.Partitions = new OffsetCommitRequestTopicPartition[1];
            var partition
                = request.Partitions[0]
                = new OffsetCommitRequestTopicPartition();
            partition.TopicName = DemoTopic;
            partition.Details = new OffsetCommitRequestTopicPartitionDetail[1];
            var detail
                = partition.Details[0]
                = new OffsetCommitRequestTopicPartitionDetail();
            detail.Partition = 0;
            detail.Offset = 1;

            var response = (OffsetCommitResponse)InvokeRequest(request);
        }

        static void ProceedOffsetFetch() {
            var request = new OffsetFetchRequest();
            request.ConsumerGroup = "default";
            request.TopicPartitions = new OffsetFetchRequestTopicPartition[1];
            var topicPartition
                = request.TopicPartitions[0]
                = new OffsetFetchRequestTopicPartition();
            topicPartition.TopicName = DemoTopic;
            topicPartition.Partitions = new[] { 0 };

            var response = (OffsetFetchResponse)InvokeRequest(request);
        }

        static Response InvokeRequest(Request request) {
            var client = new Client(Option.LoadDefault());
            using (var responseDispatcher = client.Send(request)) {
                return responseDispatcher.ParseResult();
            }
        }

        #endregion
    }
}
