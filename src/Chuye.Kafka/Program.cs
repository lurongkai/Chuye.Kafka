using System;
using System.Diagnostics;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;
using Chuye.Kafka.Protocol.Implement.Management;
using Chuye.Kafka.Utils;

namespace Chuye.Kafka {
    class Program {
        const String DemoTopic = "demo-topic";

        static void Main(string[] args) {
            //ProduceDemo();
            ConsumerDemo();

            if (Debugger.IsAttached) {
                Console.WriteLine("Press <Enter> to exit");
                Console.ReadLine();
            }
        }

        #region high level api demo
        static void ConsumerDemo() {
            var option = Option.LoadDefault();
            var consumer = new Consumer(option);
            var metadata = consumer.FetchMetadata(DemoTopic);
            var offset = consumer.FetchOffset(DemoTopic);
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
            {
                var request = new ListGroupsRequest();
                var buffer = InvokeRequest(request);
                var response = new ListGroupsResponse();
                response.Read(buffer);
            }

            {
                var request = new DescribeGroupsRequest();
                request.GroupId = new[] { "xx" };
                var buffer = InvokeRequest(request);
                var response = new DescribeGroupsResponse();
                response.Read(buffer);
            }
        }

        static void ProceedGroupCoordinator() {
            var request = new GroupCoordinatorRequest();
            //request.GroupId = String.Empty;
            request.GroupId = "100";

            var buffer = InvokeRequest(request);
            var response = new GroupCoordinatorResponse();
            response.Read(buffer);
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


            var buffer = InvokeRequest(request);
            var response = new OffsetCommitResponse();
            response.Read(buffer);
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

            var buffer = InvokeRequest(request);
            var response = new OffsetFetchResponse();
            response.Read(buffer);
        }

        static ArraySegment<Byte> InvokeRequest(Request request) {
            var bufferProvider = new BufferProvider();
            using (var socket = new Socket(SocketType.Stream, ProtocolType.Tcp))
            using (var requestBuffer = bufferProvider.Borrow())
            using (var responseBuffer = bufferProvider.Borrow()) {
                var requestBytes = request.Serialize(requestBuffer.Buffer);
                //Console.WriteLine("Sending: {0}", BitConverter.ToString(buffer.Array, 0, buffer.Offset));
                //var str1 = Encoding.UTF8.GetString(buffer.Array, 0, buffer.Offset);
                //Console.WriteLine("Parsed: {0}", Regex.Replace(str1, "[^a-zA-Z0-9]+", " "));


                //socket.Connect("192.168.8.130", 9092);
                socket.Connect("127.0.0.1", 9092); // a proxy outside for debug
                socket.Send(requestBytes.Array, requestBytes.Offset, SocketFlags.None);

                const Int32 lengthBytesSize = 4;
                var beginningBytesReceived = socket.Receive(responseBuffer.Buffer, lengthBytesSize, SocketFlags.None);
                if (beginningBytesReceived < lengthBytesSize) {
                    throw new SocketException((Int32)SocketError.SocketError);
                }
                var expectedBodyBytesSize = new Reader(responseBuffer.Buffer).ReadInt32();
                Console.WriteLine("Expected body bytes size is {0}", expectedBodyBytesSize);

                var receivedBodyBytesSize = 0;
                //while (socket.Available > 0)  //failure

                while (receivedBodyBytesSize < expectedBodyBytesSize) {
                    receivedBodyBytesSize += socket.Receive(
                        responseBuffer.Buffer,
                        receivedBodyBytesSize + lengthBytesSize,
                        expectedBodyBytesSize - receivedBodyBytesSize,
                        SocketFlags.None
                    );
                    Console.WriteLine("Actually body bytes received {0}", receivedBodyBytesSize);
                }

                //Console.WriteLine("Actually body bytes received {0}", receivedBodyBytesSize);
                var str2 = Encoding.UTF8.GetString(responseBuffer.Buffer, 0, lengthBytesSize + receivedBodyBytesSize);
                Console.WriteLine("Parsed: {0}", Regex.Replace(str2, "[^a-zA-Z0-9]+", " "));

                socket.Close();
                return new ArraySegment<Byte>(responseBuffer.Buffer, 0, lengthBytesSize + receivedBodyBytesSize);
            }
        }

        #endregion
    }
}
