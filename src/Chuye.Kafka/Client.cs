using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.ServiceModel.Channels;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;

namespace Chuye.Kafka {
    public class Client : IDisposable {
        private readonly Option _option;
        private readonly BufferManager _bufferManager;
        private readonly SocketManager _socketManager;
        private readonly Int32 _blockBufferSize;

        public Client(Option option) {
            _option = option;
            _blockBufferSize = option.BlockBufferSize;
            _bufferManager = BufferManager.CreateBufferManager(option.MaxBufferSize, option.MaxBufferSize);
            _socketManager = new SocketManager();
        }

        public IResponseDispatcher Send(Request request) {
            Socket socket = null;
            Byte[] requestBytes = null;
            try {
                socket = _socketManager.Obtain();
                requestBytes = _bufferManager.TakeBuffer(_blockBufferSize);
                var requestBytesCount = request.Serialize(new ArraySegment<Byte>(requestBytes));
                if (!socket.Connected) {
                    socket.Connect(_option.Host, _option.Port);
                }
                socket.Send(requestBytes, 0, requestBytesCount, SocketFlags.None);
                var produceRequest = request as ProduceRequest;
                if (produceRequest != null && produceRequest.RequiredAcks == AcknowlegeStrategy.Immediate) {
                    return null;
                }

                const Int32 lengthBytesSize = 4;
                var responseBytes = _bufferManager.TakeBuffer(_blockBufferSize);
                var beginningBytesReceived = socket.Receive(responseBytes, 0, lengthBytesSize, SocketFlags.None);
                if (beginningBytesReceived < lengthBytesSize) {
                    throw new SocketException((Int32)SocketError.SocketError);
                }
                var expectedBodyReader = new Reader(responseBytes, 0);
                var expectedBodyBytesSize = expectedBodyReader.ReadInt32();
                Debug.WriteLine("Expected body bytes size is {0}", expectedBodyBytesSize);
                var receivedBodyBytesSize = 0;

                while (receivedBodyBytesSize < expectedBodyBytesSize) {
                    receivedBodyBytesSize += socket.Receive(responseBytes,
                        lengthBytesSize + receivedBodyBytesSize,
                        expectedBodyBytesSize - receivedBodyBytesSize,
                        SocketFlags.None);
                    Debug.WriteLine("Actually body bytes received {0}", receivedBodyBytesSize);
                }
                return new ReponseDispatcher(request.ApiKey, responseBytes, _bufferManager);
            }
            finally {
                if (socket != null) {
                    _socketManager.Release(socket);
                }
                if (requestBytes != null) {
                    _bufferManager.ReturnBuffer(requestBytes);
                }
            }
        }

        public IResponseDispatcher Send_(Request request) {
            Byte[] requestBytes = null;
            try {
                requestBytes = _bufferManager.TakeBuffer(_blockBufferSize);
                var requestBytesCount = request.Serialize(new ArraySegment<byte>(requestBytes));
                using (var tcpClient = new TcpClient(_option.Host, _option.Port))
                using (var stream = tcpClient.GetStream()) {
                    stream.Write(requestBytes, 0, requestBytesCount);
                    var produceRequest = request as ProduceRequest;
                    if (produceRequest != null && produceRequest.RequiredAcks == AcknowlegeStrategy.Immediate) {
                        return null;
                    }

                    const Int32 lengthBytesSize = 4;
                    var responseBytes = _bufferManager.TakeBuffer(_blockBufferSize);
                    var beginningBytesReceived = stream.Read(responseBytes, 0, lengthBytesSize);

                    if (beginningBytesReceived < lengthBytesSize) {
                        throw new SocketException((Int32)SocketError.SocketError);
                    }
                    var expectedBodyReader = new Reader(responseBytes, 0);
                    var expectedBodyBytesSize = expectedBodyReader.ReadInt32();
                    Debug.WriteLine("Expected body bytes size is {0}", expectedBodyBytesSize);
                    var receivedBodyBytesSize = 0;

                    while (receivedBodyBytesSize < expectedBodyBytesSize) {
                        receivedBodyBytesSize += stream.Read(responseBytes,
                            lengthBytesSize + receivedBodyBytesSize,
                            expectedBodyBytesSize - receivedBodyBytesSize);
                        Debug.WriteLine("Actually body bytes received {0}", receivedBodyBytesSize);
                    }

                    //var responseBytes = new Byte[lengthBytesSize + receivedBodyBytesSize];
                    //Array.Copy(responseBuffer.Segment.Array, responseBuffer.Segment.Offset, responseBytes, 0, responseBytes.Length);
                    return new ReponseDispatcher(request.ApiKey, responseBytes, _bufferManager);
                }
            }
            finally {
                if (requestBytes != null) {
                    _bufferManager.ReturnBuffer(requestBytes);
                }
            }
        }

        public async Task<IResponseDispatcher> SendAsync(Request request) {
            Byte[] requestBytes = null;
            try {
                requestBytes = _bufferManager.TakeBuffer(_blockBufferSize);
                var requestBytesCount = request.Serialize(new ArraySegment<byte>(requestBytes));
                using (var tcpClient = new TcpClient(_option.Host, _option.Port))
                using (var stream = tcpClient.GetStream()) {
                    await stream.WriteAsync(requestBytes, 0, requestBytesCount);
                    var produceRequest = request as ProduceRequest;
                    if (produceRequest != null && produceRequest.RequiredAcks == AcknowlegeStrategy.Immediate) {
                        stream.Close();
                        return null;
                    }

                    const Int32 lengthBytesSize = 4;
                    var responseBytes = _bufferManager.TakeBuffer(_blockBufferSize);
                    var beginningBytesReceived = await stream.ReadAsync(responseBytes, 0, lengthBytesSize);
                    if (beginningBytesReceived < lengthBytesSize) {
                        throw new SocketException((Int32)SocketError.SocketError);
                    }
                    var expectedBodyReader = new Reader(responseBytes, 0);
                    var expectedBodyBytesSize = expectedBodyReader.ReadInt32();
                    Debug.WriteLine("Expected body bytes size is {0}", expectedBodyBytesSize);
                    var receivedBodyBytesSize = 0;

                    while (receivedBodyBytesSize < expectedBodyBytesSize) {
                        receivedBodyBytesSize += await stream.ReadAsync(responseBytes,
                            lengthBytesSize + receivedBodyBytesSize,
                            expectedBodyBytesSize - receivedBodyBytesSize);
                        Debug.WriteLine("Actually body bytes received {0}", receivedBodyBytesSize);
                    }

                    //var responseBytes = new Byte[lengthBytesSize + receivedBodyBytesSize];
                    //Array.Copy(responseBuffer.Segment.Array, responseBuffer.Segment.Offset, responseBytes, 0, responseBytes.Length);
                    return new ReponseDispatcher(request.ApiKey, responseBytes, _bufferManager);
                }
            }
            finally {
                if (requestBytes != null) {
                    _bufferManager.ReturnBuffer(requestBytes);
                }
            }
        }

        public void Dispose() {
            _socketManager.Dispose();
            _bufferManager.Clear();
        }
    }
}
