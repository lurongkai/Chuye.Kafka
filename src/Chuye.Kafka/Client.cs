using System;
using System.Diagnostics;
using System.Net.Sockets;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;

namespace Chuye.Kafka {
    public class Client {
        private readonly String _host;
        private readonly Int32 _port;
        private readonly IBufferManager _bufferManager;

        public Client(Option option)
            : this(option.Host, option.Port) {
        }

        public Client(String host, Int32 port) {
            _host = host;
            _port = port;
            _bufferManager = new BufferManager(4096, 10);
        }

        public IResponseDispatcher Send(Request request) {
            using (var socket = new Socket(SocketType.Stream, ProtocolType.Tcp))
            using (var requestBuffer = _bufferManager.Borrow()) {
                var requestBytes = request.Serialize(requestBuffer.Segment);
                socket.Connect(_host, _port);
                socket.Send(requestBuffer.Segment.Array, requestBuffer.Segment.Offset,
                    requestBytes.Count, SocketFlags.None);

                var produceRequest = request as ProduceRequest;
                if (produceRequest != null && produceRequest.RequiredAcks == AcknowlegeStrategy.Async) {
                    socket.Close();
                    return null;
                }

                const Int32 lengthBytesSize = 4;
                var responseBuffer = _bufferManager.Borrow();
                var beginningBytesReceived = socket.Receive(
                    responseBuffer.Segment.Array, responseBuffer.Segment.Offset,
                    lengthBytesSize, SocketFlags.None);
                if (beginningBytesReceived < lengthBytesSize) {
                    throw new SocketException((Int32)SocketError.SocketError);
                }
                var expectedBodyReader = new Reader(responseBuffer.Segment);
                var expectedBodyBytesSize = expectedBodyReader.ReadInt32();
                Debug.WriteLine("Expected body bytes size is {0}", expectedBodyBytesSize);
                var receivedBodyBytesSize = 0;

                while (receivedBodyBytesSize < expectedBodyBytesSize) {
                    receivedBodyBytesSize += socket.Receive(
                        responseBuffer.Segment.Array,
                        responseBuffer.Segment.Offset + lengthBytesSize + receivedBodyBytesSize,
                        expectedBodyBytesSize - receivedBodyBytesSize,
                        SocketFlags.None
                    );
                    Debug.WriteLine("Actually body bytes received {0}", receivedBodyBytesSize);
                }

                socket.Close();
                var responseBytes = new Byte[lengthBytesSize + receivedBodyBytesSize];
                Array.Copy(responseBuffer.Segment.Array, responseBuffer.Segment.Offset, responseBytes, 0, responseBytes.Length);
                return new ReponseDispatcher(request.ApiKey, responseBuffer);
            }
        }
    }
}
