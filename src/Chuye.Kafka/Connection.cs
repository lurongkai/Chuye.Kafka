﻿using System;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.ServiceModel.Channels;
using System.Threading;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka {
    public interface IRouter {
        IConnection Route(String topicName);
        Int32 CurrentPartition { get; }
    }

    public interface IConnection: IRouter {
        IResponseDispatcher Send(Request request);
        Task<IResponseDispatcher> SendAsync(Request request);
    }

    public class Statistic {
        private Int64 _byteSended;
        private Int64 _byteReceived;
        private Int64 _requestSended;
        private Int64 _responseRecieved;

        public Int64 ByteSended {
            get { return _byteSended; }
        }
        public Int64 ByteReceived {
            get { return _byteReceived; }
        }
        public Int64 RequestSended {
            get { return _requestSended; }
        }
        public Int64 ResponseRecieved {
            get { return _responseRecieved; }
        }

        public void IncreaseByteSended(Int64 bytesSize) {
            Interlocked.Add(ref _byteSended, bytesSize);
        }

        public void IncreaseByteReceived(Int64 bytesSize) {
            Interlocked.Add(ref _byteReceived, bytesSize);
        }

        public void IncreaseRequestSended() {
            Interlocked.Increment(ref _requestSended);
        }

        public void IncreaseResponseRecieved() {
            Interlocked.Increment(ref _responseRecieved);
        }
    }

    public class Connection : IConnection, IDisposable {
        private readonly KafkaConfigurationSection _section;
        private readonly BufferManager _bufferManager;
        private readonly SocketManager _socketManager;

        public static Statistic Statistic { get; private set; }
        public Int32 CurrentPartition { get; internal set; }

        static Connection() {
            Statistic = new Statistic();
        }

        internal KafkaConfigurationSection Section {
            get { return _section; }
        }

        public Connection()
            : this(KafkaConfigurationSection.LoadDefault()) {
        }

        public Connection(KafkaConfigurationSection section) {
            if (section == null) {
                throw new ArgumentNullException("section");
            }

            _section = section;
            _bufferManager = BufferManager.CreateBufferManager(section.Buffer.MaxBufferPoolSize, section.Buffer.MaxBufferSize);
            _socketManager = new SocketManager();
        }

        protected Connection(String host, Int32 port, BufferManager bufferManager, SocketManager socketManager) {            
            _section = new KafkaConfigurationSection(host, port);
            _bufferManager = bufferManager;
            _socketManager = socketManager;
        }

        public virtual IConnection Route(String topicName) {
            return this;
        }

        protected Connection Clone(String host, Int32 port) {
            return new Connection(host, port, _bufferManager, _socketManager);
        }

        public virtual TopicMetadataResponse TopicMetadata(params String[] topicNames) {
            if (topicNames == null) {
                throw new ArgumentOutOfRangeException("topicNames");
            }

            var request = new TopicMetadataRequest();
            request.TopicNames = topicNames;
            var response = (TopicMetadataResponse)Invoke(request);
            var errors = response.TopicMetadatas
                .Where(x => x.TopicErrorCode != ErrorCode.NoError);
            if (errors.Any()) {
                throw new KafkaException(errors.First().TopicErrorCode);
            }
            return response;
        }

        public IResponseDispatcher Send(Request request) {
            ConnectedSocket socket = null;
            Byte[] requestBytes = null;
            try {
                socket = _socketManager.Obtain(_section.Broker.Host, _section.Broker.Port);
                requestBytes = _bufferManager.TakeBuffer(_section.Buffer.RequestBufferSize);
                var requestBytesCount = request.Serialize(requestBytes, 0);
                //Debug.WriteLine(String.Join(" ", requestBytes.Take(requestBytesCount)));
                socket.Socket.Send(requestBytes, 0, requestBytesCount, SocketFlags.None);
                Statistic.IncreaseRequestSended();
                Statistic.IncreaseByteSended(requestBytesCount);

                var produceRequest = request as ProduceRequest;
                if (produceRequest != null && produceRequest.RequiredAcks == AcknowlegeStrategy.Immediate) {
                    return null;
                }

                const Int32 lengthBytesSize = 4;
                var responseBytes = _bufferManager.TakeBuffer(_section.Buffer.ResponseBufferSize);
                var beginningBytesReceived = socket.Socket.Receive(responseBytes, 0, lengthBytesSize, SocketFlags.None);
                if (beginningBytesReceived < lengthBytesSize) {
                    throw new SocketException((Int32)SocketError.SocketError);
                }
                var expectedBodyReader = new BufferReader(responseBytes, 0);
                var expectedBodyBytesSize = expectedBodyReader.ReadInt32();
                //Debug.WriteLine("Expected body bytes size is {0}", expectedBodyBytesSize);
                var receivedBodyBytesSize = 0;

                while (receivedBodyBytesSize < expectedBodyBytesSize) {
                    receivedBodyBytesSize += socket.Socket.Receive(responseBytes,
                        lengthBytesSize + receivedBodyBytesSize,
                        expectedBodyBytesSize - receivedBodyBytesSize,
                        SocketFlags.None);
                    //Debug.WriteLine("Actually body bytes received {0}", receivedBodyBytesSize);
                }
                Statistic.IncreaseResponseRecieved();
                Statistic.IncreaseByteReceived(beginningBytesReceived + receivedBodyBytesSize);
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
        
        public async Task<IResponseDispatcher> SendAsync(Request request) {
            Byte[] requestBytes = null;
            try {
                requestBytes = _bufferManager.TakeBuffer(_section.Buffer.RequestBufferSize);
                var requestBytesCount = request.Serialize(requestBytes, 0);
                using (var tcpClient = new TcpClient(_section.Broker.Host, _section.Broker.Port))
                using (var stream = tcpClient.GetStream()) {
                    await stream.WriteAsync(requestBytes, 0, requestBytesCount);
                    Statistic.IncreaseRequestSended();
                    Statistic.IncreaseByteSended(requestBytesCount);
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
                    Statistic.IncreaseResponseRecieved();
                    Statistic.IncreaseByteReceived(beginningBytesReceived + receivedBodyBytesSize);
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
