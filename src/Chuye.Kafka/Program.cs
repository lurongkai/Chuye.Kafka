using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;
using Chuye.Kafka.Protocol.Implement.Management;
using Chuye.Kafka.Utils;

namespace Chuye.Kafka {
    class Program {
        const String TopicName = "test6";

        static void Main(string[] args) {
            //ProceedMetadata();
            //ProceedProduce();
            //ProceedFetch();
            //ProceedOffset();
            //ProceedGroupCoordinator();
            //ProceedOffsetCommit();
            //ProceedOffsetFetch();
            ProceedListGroups_DescribeGroups();

            if (Debugger.IsAttached) {
                Console.WriteLine("Press <Enter> to exit");
                Console.ReadLine();
            }
        }

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

        static void ProceedMetadata() {
            var request = new MetadataRequest();
            request.TopicNames = new[] { TopicName };

            var buffer = InvokeRequest(request);
            var response = new MetadataResponse();
            response.Read(buffer);
        }

        static void ProceedProduce() {
            var request = new ProduceRequest();
            request.RequiredAcks = 1;
            request.Timeout = 100;
            request.TopicPartitions = new ProduceRequestTopicPartition[1];
            var topicPartition
                = request.TopicPartitions[0]
                = new ProduceRequestTopicPartition();
            topicPartition.TopicName = TopicName;
            topicPartition.Details = new ProduceRequestTopicDetail[1];
            var topicDetail
                = topicPartition.Details[0]
                = new ProduceRequestTopicDetail();
            topicDetail.MessageSets = new MessageSetCollection();
            topicDetail.MessageSets.Items = new MessageSet[1];
            var messageSet
                = topicDetail.MessageSets.Items[0]
                = new MessageSet();
            messageSet.Message = new Message();
            messageSet.Message.Key = Encoding.UTF8.GetBytes("yeah");
            messageSet.Message.Value = Encoding.UTF8.GetBytes("Hello world " + Guid.NewGuid().ToString("n"));

            var buffer = InvokeRequest(request);
            var response = new ProduceResponse();
            response.Read(buffer);
        }

        static void ProceedFetch() {
            var request = new FetchRequest();
            request.ReplicaId = -1;
            //e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k 
            // would allow the server to wait up to 100ms  
            // to try to accumulate 64k of data before responding
            request.MaxWaitTime = 100;
            request.MinBytes = 64 * 1024;
            request.TopicPartitions = new TopicPartition[1];
            var topicPartition
                = request.TopicPartitions[0]
                = new TopicPartition();
            topicPartition.TopicName = TopicName;
            topicPartition.FetchOffsetDetails = new FetchOffsetDetail[1];
            var fetchOffsetDetail
                = topicPartition.FetchOffsetDetails[0]
                = new FetchOffsetDetail();
            fetchOffsetDetail.FetchOffset = 0L;
            fetchOffsetDetail.MaxBytes = 64 * 1024;

            var buffer = InvokeRequest(request);
            var response = new FetchResponse();
            response.Read(buffer);

            //var jsonStr = Newtonsoft.Json.JsonConvert.SerializeObject(response);
            //Console.WriteLine(jsonStr);
        }

        static void ProceedOffset() {
            var request = new OffsetRequest();
            request.ReplicaId = 0;
            request.Partitions = new OffsetsRequestTopicPartition[1];
            var partition
                = request.Partitions[0]
                = new OffsetsRequestTopicPartition();
            partition.TopicName = TopicName;
            partition.Details = new OffsetsRequestTopicPartitionDetail[1];
            var detail
                = partition.Details[0]
                = new OffsetsRequestTopicPartitionDetail();
            detail.Partition = 0;
            detail.Time = -1;
            detail.MaxNumberOfOffsets = 1;

            var buffer = InvokeRequest(request);
            var response = new OffsetResponse();
            response.Read(buffer);
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
            partition.TopicName = TopicName;
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
            topicPartition.TopicName = TopicName;
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
    }
}
