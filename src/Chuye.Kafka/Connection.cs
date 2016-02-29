using System;
using System.Linq;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.ServiceModel.Channels;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;
using Chuye.Kafka.Serialization;
using System.Collections.Generic;

namespace Chuye.Kafka {
    public interface IRouter {
        IConnection Route(String topicName, out Int32 partitionId);
    }
    
    public interface IConnection {
        IResponseDispatcher Send(Request request);
        Task<IResponseDispatcher> SendAsync(Request request);
    }

    public class Router : Connection, IRouter {
        private readonly Random _random;
        private HashSet<Broker> _brokers;
        private HashSet<TopicMetadata> _topics;

        public Router(Option option)
            : base(option) {
            _random = new Random();
            _brokers = new HashSet<Broker>();
            _topics = new HashSet<TopicMetadata>();
        }

        public IConnection Route(String topicName, out Int32 partitionId) {
            var topic = _topics.Where(r => r.TopicName.Equals(topicName)).SingleOrDefault();
            if (topic == null) {
                var resp = TopicMetadata(topicName);
                foreach (var item in resp.Brokers) {
                    _brokers.Add(item);
                }
                foreach (var item in resp.TopicMetadatas) {
                    _topics.Add(item);
                }
                topic = resp.TopicMetadatas.Where(r => r.TopicName.Equals(topicName)).SingleOrDefault();
            }

            var partition = topic.PartitionMetadatas[_random.Next(topic.PartitionMetadatas.Length)];
            var broker = _brokers.SingleOrDefault(b => b.NodeId == partition.Leader);
            var option = new Option(broker.Host, broker.Port);
            partitionId = partition.PartitionId;
            return Clone(option);
        }

        public override TopicMetadataResponse TopicMetadata(String topicName) {
            var attemptLimit = 5;
            var response = base.TopicMetadata(topicName);
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
                    response = base.TopicMetadata(topicName);
                }
                else {
                    throw new KafkaException(metadata.TopicErrorCode);
                }
            }
            return response;
        }
    }

    public class Connection : IConnection, IDisposable {
        private readonly Option _option;
        private readonly IPEndPoint _endPoint;
        private readonly BufferManager _bufferManager;
        private readonly SocketManager _socketManager;

        public Connection(Option option) {
            _option = option;
            _endPoint = _option;
            _bufferManager = BufferManager.CreateBufferManager(option.MaxBufferSize, option.MaxBufferSize);
            _socketManager = new SocketManager();
        }

        private Connection(Option option, BufferManager bufferManager, SocketManager socketManager) {
            _option = option;
            _endPoint = option;
            _bufferManager = bufferManager;
            _socketManager = socketManager;
        }

        protected Connection Clone(Option option) {
            return new Connection(option, _bufferManager, _socketManager);
        }

        public virtual TopicMetadataResponse TopicMetadata(String topicName) {
            var request = new TopicMetadataRequest();
            request.TopicNames = new[] { topicName };
            var response = (TopicMetadataResponse)Invoke(request);
            var errors = response.TopicMetadatas
                .Where(x => x.TopicErrorCode != ErrorCode.NoError);
            if (errors.Any()) {
                throw new KafkaException(errors.First().TopicErrorCode);
            }
            return response;
        }

        public IResponseDispatcher Send(Request request) {
            Socket socket = null;
            Byte[] requestBytes = null;
            try {
                socket = _socketManager.Obtain(_endPoint);
                if (!socket.Connected) {
                    socket.Connect(_endPoint);
                }

                requestBytes = _bufferManager.TakeBuffer(_option.RequestBufferSize);
                var requestBytesCount = request.Serialize(requestBytes, 0);

                //Debug.WriteLine(String.Join(" ", requestBytes.Take(requestBytesCount)));
                socket.Send(requestBytes, 0, requestBytesCount, SocketFlags.None);

                //var hostEntry = Dns.GetHostEntry(_option.Host);
                //var endPoint = new IPEndPoint(hostEntry.AddressList[0], _option.Port);
                //socket.SendTo(requestBytes, 0, requestBytesCount, SocketFlags.None, endPoint);

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
                using (var tcpClient = new TcpClient(_endPoint))
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
                using (var tcpClient = new TcpClient(_endPoint))
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

        public Response Invoke(Request request) {
            using (var responseDispatcher = Send(request)) {
                return responseDispatcher.ParseResult();
            }
        }

        public async Task<Response> InvokeAsync(Request request) {
            using (var responseDispatcher = await SendAsync(request)) {
                return responseDispatcher.ParseResult();
            }
        }

        public void Dispose() {
            _socketManager.Dispose();
            _bufferManager.Clear();
        }
    }
}
