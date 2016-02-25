using System;
using System.Diagnostics;
using System.Net.Sockets;
using System.ServiceModel.Channels;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka {
    public class Connection : IDisposable {
        private readonly Option _option;
        private readonly BufferManager _bufferManager;
        private readonly SocketManager _socketManager;

        public Connection(Option option) {
            _option = option;
            _bufferManager = BufferManager.CreateBufferManager(option.MaxBufferSize, option.MaxBufferSize);
            _socketManager = new SocketManager();
        }


        public TopicMetadataResponse TopicMetadata(String topicName) {
            var request = new TopicMetadataRequest();
            request.TopicNames = new[] { topicName };
            var attemptLimit = 5;
            var response = (TopicMetadataResponse)Invoke(request);
            while (attemptLimit-- > 0) {
                var metadata = response.TopicMetadatas[0];
                if (metadata.TopicErrorCode == ErrorCode.NoError) {
                    break;
                }
                if (metadata.TopicErrorCode == ErrorCode.LeaderNotAvailable) {
                    Debug.WriteLine("LeaderNotAvailable while hanlde TopicMetadata(\"{0}\")", args: topicName);
                    if (attemptLimit <= 0) {
                        throw new KafkaException(metadata.TopicErrorCode);
                    }
                    Thread.Sleep(50);
                    response = (TopicMetadataResponse)Invoke(request);
                }
                else {
                    throw new KafkaException(metadata.TopicErrorCode);
                }
            }
            return response;
        }

        public Response Invoke(TopicMetadataRequest request) {
            using (var responseDispatcher = Send(request)) {
                return responseDispatcher.ParseResult();
            }
        }

        public IResponseDispatcher Send(Request request) {
            Socket socket = null;
            Byte[] requestBytes = null;
            try {
                socket = _socketManager.Obtain();
                requestBytes = _bufferManager.TakeBuffer(_option.RequestBufferSize);
                var requestBytesCount = request.Serialize(requestBytes, 0);
                if (!socket.Connected) {
                    socket.Connect(_option.Host, _option.Port);
                }
                socket.Send(requestBytes, 0, requestBytesCount, SocketFlags.None);
                var produceRequest = request as ProduceRequest;
                if (produceRequest != null && produceRequest.RequiredAcks == AcknowlegeStrategy.Immediate) {
                    return null;
                }

                const Int32 lengthBytesSize = 4;
                var responseBytes = _bufferManager.TakeBuffer(_option.ResponseBufferSize);
                var beginningBytesReceived = socket.Receive(responseBytes, 0, lengthBytesSize, SocketFlags.None);
                if (beginningBytesReceived < lengthBytesSize) {
                    throw new SocketException((Int32)SocketError.SocketError);
                }
                var expectedBodyReader = new BufferReader(responseBytes, 0);
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
                requestBytes = _bufferManager.TakeBuffer(_option.RequestBufferSize);
                var requestBytesCount = request.Serialize(requestBytes, 0);
                using (var tcpClient = new TcpClient(_option.Host, _option.Port))
                using (var stream = tcpClient.GetStream()) {
                    stream.Write(requestBytes, 0, requestBytesCount);
                    var produceRequest = request as ProduceRequest;
                    if (produceRequest != null && produceRequest.RequiredAcks == AcknowlegeStrategy.Immediate) {
                        return null;
                    }

                    const Int32 lengthBytesSize = 4;
                    var responseBytes = _bufferManager.TakeBuffer(_option.ResponseBufferSize);
                    var beginningBytesReceived = stream.Read(responseBytes, 0, lengthBytesSize);

                    if (beginningBytesReceived < lengthBytesSize) {
                        throw new SocketException((Int32)SocketError.SocketError);
                    }
                    var expectedBodyReader = new BufferReader(responseBytes, 0);
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
                requestBytes = _bufferManager.TakeBuffer(_option.RequestBufferSize);
                var requestBytesCount = request.Serialize(requestBytes, 0);
                using (var tcpClient = new TcpClient(_option.Host, _option.Port))
                using (var stream = tcpClient.GetStream()) {
                    await stream.WriteAsync(requestBytes, 0, requestBytesCount);
                    var produceRequest = request as ProduceRequest;
                    if (produceRequest != null && produceRequest.RequiredAcks == AcknowlegeStrategy.Immediate) {
                        stream.Close();
                        return null;
                    }

                    const Int32 lengthBytesSize = 4;
                    var responseBytes = _bufferManager.TakeBuffer(_option.ResponseBufferSize);
                    var beginningBytesReceived = await stream.ReadAsync(responseBytes, 0, lengthBytesSize);
                    if (beginningBytesReceived < lengthBytesSize) {
                        throw new SocketException((Int32)SocketError.SocketError);
                    }
                    var expectedBodyReader = new BufferReader(responseBytes, 0);
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
