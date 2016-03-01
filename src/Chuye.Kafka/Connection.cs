using System;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.ServiceModel.Channels;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka {
    public interface IRouter {
        Int32 CurrentPartition { get; }
        IConnection Route(String topicName);
    }

    public interface IConnection: IRouter {
        IResponseDispatcher Send(Request request);
        Task<IResponseDispatcher> SendAsync(Request request);
    }

    public class Connection : IConnection, IDisposable {

        private const String IpRegexPattern = "\\b(?:\\d{1,3}\\.){3}\\d{1,3}\\b";

        private readonly KafkaConfigurationSection _section;
        private readonly IPEndPoint _endPoint;
        private readonly BufferManager _bufferManager;
        private readonly SocketManager _socketManager;

        public Int32 CurrentPartition { get; protected set; }

        public Connection()
            : this(KafkaConfigurationSection.LoadDefault()) {
        }

        public Connection(KafkaConfigurationSection section) {
            if (section == null) {
                throw new ArgumentNullException("section");
            }

            var addresses = Dns.GetHostAddresses(section.Broker.Host);
            if (addresses.Length == 0) {
                throw new ConfigurationErrorsException("Server ip abtain failure");
            }
            _section = section;
            _endPoint = new IPEndPoint(addresses[0], section.Broker.Port);
            _bufferManager = BufferManager.CreateBufferManager(section.Buffer.MaxBufferPoolSize,
                section.Buffer.MaxBufferSize);
            _socketManager = new SocketManager();
        }

        protected Connection(String host, Int32 port, Int32 partition, BufferManager bufferManager, SocketManager socketManager) {
            var addresses = Dns.GetHostAddresses(host);
            if (addresses.Length == 0) {
                throw new ConfigurationErrorsException("Server ip abtain failure");
            }
            _section = new KafkaConfigurationSection(host, port);
            _endPoint = new IPEndPoint(addresses[0], port);
            CurrentPartition = partition;
            _bufferManager = bufferManager;
            _socketManager = socketManager;
        }

        public virtual IConnection Route(String topicName) {
            return this;
        }

        protected Connection Clone(String host, Int32 port, Int32 partition) {
            return new Connection(host, port, partition, _bufferManager, _socketManager);
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

                requestBytes = _bufferManager.TakeBuffer(_section.Buffer.RequestBufferSize);
                var requestBytesCount = request.Serialize(requestBytes, 0);
                //Debug.WriteLine(String.Join(" ", requestBytes.Take(requestBytesCount)));
                socket.Send(requestBytes, 0, requestBytesCount, SocketFlags.None);

                var produceRequest = request as ProduceRequest;
                if (produceRequest != null && produceRequest.RequiredAcks == AcknowlegeStrategy.Immediate) {
                    return null;
                }

                const Int32 lengthBytesSize = 4;
                var responseBytes = _bufferManager.TakeBuffer(_section.Buffer.ResponseBufferSize);
                var beginningBytesReceived = socket.Receive(responseBytes, 0, lengthBytesSize, SocketFlags.None);
                if (beginningBytesReceived < lengthBytesSize) {
                    throw new SocketException((Int32)SocketError.SocketError);
                }
                var expectedBodyReader = new BufferReader(responseBytes, 0);
                var expectedBodyBytesSize = expectedBodyReader.ReadInt32();
                //Debug.WriteLine("Expected body bytes size is {0}", expectedBodyBytesSize);
                var receivedBodyBytesSize = 0;

                while (receivedBodyBytesSize < expectedBodyBytesSize) {
                    receivedBodyBytesSize += socket.Receive(responseBytes,
                        lengthBytesSize + receivedBodyBytesSize,
                        expectedBodyBytesSize - receivedBodyBytesSize,
                        SocketFlags.None);
                    //Debug.WriteLine("Actually body bytes received {0}", receivedBodyBytesSize);
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
                requestBytes = _bufferManager.TakeBuffer(_section.Buffer.RequestBufferSize);
                var requestBytesCount = request.Serialize(requestBytes, 0);
                using (var tcpClient = new TcpClient(_endPoint))
                using (var stream = tcpClient.GetStream()) {
                    stream.Write(requestBytes, 0, requestBytesCount);
                    var produceRequest = request as ProduceRequest;
                    if (produceRequest != null && produceRequest.RequiredAcks == AcknowlegeStrategy.Immediate) {
                        return null;
                    }

                    const Int32 lengthBytesSize = 4;
                    var responseBytes = _bufferManager.TakeBuffer(_section.Buffer.ResponseBufferSize);
                    var beginningBytesReceived = stream.Read(responseBytes, 0, lengthBytesSize);

                    if (beginningBytesReceived < lengthBytesSize) {
                        throw new SocketException((Int32)SocketError.SocketError);
                    }
                    var expectedBodyReader = new BufferReader(responseBytes, 0);
                    var expectedBodyBytesSize = expectedBodyReader.ReadInt32();
                    //Debug.WriteLine("Expected body bytes size is {0}", expectedBodyBytesSize);
                    var receivedBodyBytesSize = 0;

                    while (receivedBodyBytesSize < expectedBodyBytesSize) {
                        receivedBodyBytesSize += stream.Read(responseBytes,
                            lengthBytesSize + receivedBodyBytesSize,
                            expectedBodyBytesSize - receivedBodyBytesSize);
                        //Debug.WriteLine("Actually body bytes received {0}", receivedBodyBytesSize);
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
                requestBytes = _bufferManager.TakeBuffer(_section.Buffer.RequestBufferSize);
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
                    var responseBytes = _bufferManager.TakeBuffer(_section.Buffer.ResponseBufferSize);
                    var beginningBytesReceived = await stream.ReadAsync(responseBytes, 0, lengthBytesSize);
                    if (beginningBytesReceived < lengthBytesSize) {
                        throw new SocketException((Int32)SocketError.SocketError);
                    }
                    var expectedBodyReader = new BufferReader(responseBytes, 0);
                    var expectedBodyBytesSize = expectedBodyReader.ReadInt32();
                    //Debug.WriteLine("Expected body bytes size is {0}", expectedBodyBytesSize);
                    var receivedBodyBytesSize = 0;

                    while (receivedBodyBytesSize < expectedBodyBytesSize) {
                        receivedBodyBytesSize += await stream.ReadAsync(responseBytes,
                            lengthBytesSize + receivedBodyBytesSize,
                            expectedBodyBytesSize - receivedBodyBytesSize);
                        //Debug.WriteLine("Actually body bytes received {0}", receivedBodyBytesSize);
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
