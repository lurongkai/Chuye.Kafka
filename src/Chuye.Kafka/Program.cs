using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;

namespace Chuye.Kafka {
    class Program {
        const String TopicName = "test6";

        static void Main(string[] args) {
            //ProceedMetadata();
            //ProceedProduce();
            ProceedFetch();

            if (Debugger.IsAttached) {
                Console.WriteLine("Press <Enter> to exit");
                Console.ReadLine();
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
            fetchOffsetDetail.MaxBytes = 64 * 1024;

            var buffer = InvokeRequest(request);
            var response = new FetchResponse();
            response.Read(buffer);
        }

        static ArraySegment<Byte> InvokeRequest(Request request) {
            var bufferProvider = new BufferProvider();
            var bytes = bufferProvider.Borrow();
            var buffer = request.Serialize(bytes);
            //Console.WriteLine("Sending: {0}", BitConverter.ToString(buffer.Array, 0, buffer.Offset));
            //var str1 = Encoding.UTF8.GetString(buffer.Array, 0, buffer.Offset);
            //Console.WriteLine("Parsed: {0}", Regex.Replace(str1, "[^a-zA-Z0-9]+", " "));

            using (var socket = new Socket(SocketType.Stream, ProtocolType.Tcp)) {
                //socket.Connect("192.168.8.130", 9092);
                socket.Connect("127.0.0.1", 9092); // a proxy outside for debug
                socket.Send(buffer.Array, buffer.Offset, SocketFlags.None);

                Thread.Sleep(100);
                var received = socket.Receive(bytes);
                var length = new Reader(bytes).ReadInt32();
                //todo
                while (length > received - 4) {
                    Console.WriteLine("Received {0} bytes, less than {1}", received, length);
                    //Console.WriteLine("Received: {0}", BitConverter.ToString(bytes, 0, received));
                    received += socket.Receive(bytes, length + 4 - received, SocketFlags.None);
                }


                //var str2 = Encoding.UTF8.GetString(bytes, 0, received);
                //Console.WriteLine("Parsed: {0}", Regex.Replace(str2, "[^a-zA-Z0-9]+", " "));

                bufferProvider.GiveBack(bytes);
                socket.Close();

                return new ArraySegment<Byte>(bytes, 0, received);
            }
        }
    }
}
