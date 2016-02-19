using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;

namespace Chuye.Kafka {
    class Program {
        static void Main(string[] args) {
            var request = new MetadataRequest();
            request.TopicNames = new[] { "test6" };

            var bufferProvider = new BufferProvider();
            var bytes = bufferProvider.Borrow();
            var buffer = request.Serialize(bytes);
            Console.WriteLine("Sending: {0}", BitConverter.ToString(buffer.Array, 0, buffer.Offset));
            var str1 = Encoding.UTF8.GetString(buffer.Array, 0, buffer.Offset);
            Console.WriteLine("Parsed: {0}", Regex.Replace(str1, "[^a-zA-Z0-9]+", " "));

            using(var socket = new Socket(SocketType.Stream, ProtocolType.Tcp)){
                //socket.Connect("192.168.8.130", 9092);
                socket.Connect("127.0.0.1", 9092); // a proxy outside for debug
                socket.Send(buffer.Array, buffer.Offset, SocketFlags.None);

                var received = socket.Receive(bytes);
                Console.WriteLine("Receiving: {0}", BitConverter.ToString(bytes, 0, received));
                var str2 = Encoding.UTF8.GetString(bytes, 0, received);
                Console.WriteLine("Parsed: {0}", Regex.Replace(str2, "[^a-zA-Z0-9]+", " "));

                var response = new MetadataResponse();
                response.Read(new ArraySegment<byte>(bytes));

                bufferProvider.GiveBack(bytes);
                socket.Close();
            }
        }
    }
}
