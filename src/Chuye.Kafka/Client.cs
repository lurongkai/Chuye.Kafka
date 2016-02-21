using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;
using Chuye.Kafka.Protocol.Implement.Management;

namespace Chuye.Kafka {
    public class Client {
        private readonly String _host;
        private readonly Int32 _port;

        public Client(Option option) {
            _host = option.Host;
            _port = option.Port;
        }

        public Client(String host, Int32 port) {
            _host = host;
            _port = port;
        }

        public IResponseDispatcher Send(Request request) {
            var bufferProvider = new BufferProvider();
            using (var socket = new Socket(SocketType.Stream, ProtocolType.Tcp))
            using (var requestBuffer = bufferProvider.Borrow()) {
                var requestBytes = request.Serialize(requestBuffer.Buffer);
                socket.Connect(_host, _port);
                socket.Send(requestBytes.Array, requestBytes.Offset, SocketFlags.None);
                
                var produceRequest = request as ProduceRequest;
                if (produceRequest != null && produceRequest.RequiredAcks == AcknowlegeStrategy.Async) {
                    socket.Close();
                    return null;
                }

                const Int32 lengthBytesSize = 4;
                var responseBuffer = bufferProvider.Borrow();
                var beginningBytesReceived = socket.Receive(responseBuffer.Buffer, lengthBytesSize, SocketFlags.None);
                if (beginningBytesReceived < lengthBytesSize) {
                    throw new SocketException((Int32)SocketError.SocketError);
                }
                var expectedBodyBytesSize = new Reader(responseBuffer.Buffer).ReadInt32();
                //Debug.WriteLine("Expected body bytes size is {0}", expectedBodyBytesSize);
                var receivedBodyBytesSize = 0;

                while (receivedBodyBytesSize < expectedBodyBytesSize) {
                    receivedBodyBytesSize += socket.Receive(
                        responseBuffer.Buffer,
                        receivedBodyBytesSize + lengthBytesSize,
                        expectedBodyBytesSize - receivedBodyBytesSize,
                        SocketFlags.None
                    );
                    //Debug.WriteLine("Actually body bytes received {0}", receivedBodyBytesSize);
                }

                socket.Close();
                return new ReponseDispatcher(request.ApiKey, responseBuffer);
            }
        }
    }

    public interface IResponseDispatcher : IDisposable {
        Response ParseResult();
    }

    internal class ReponseDispatcher : IResponseDispatcher {
        private static readonly Type[] _responseTyps;
        private readonly ApiKey _apiKey;
        private readonly IBufferWrapper _bufferWrapper;

        static ReponseDispatcher() {
            _responseTyps = new Type[17];
            _responseTyps[(Int32)ApiKey.ProduceRequest] = typeof(ProduceResponse);
            _responseTyps[(Int32)ApiKey.FetchRequest] = typeof(FetchResponse);
            _responseTyps[(Int32)ApiKey.OffsetRequest] = typeof(OffsetResponse);
            _responseTyps[(Int32)ApiKey.MetadataRequest] = typeof(MetadataResponse);
            _responseTyps[(Int32)ApiKey.OffsetCommitRequest] = typeof(OffsetCommitResponse);
            _responseTyps[(Int32)ApiKey.OffsetFetchRequest] = typeof(OffsetFetchResponse);
            _responseTyps[(Int32)ApiKey.GroupCoordinatorRequest] = typeof(GroupCoordinatorResponse);
            _responseTyps[(Int32)ApiKey.JoinGroupRequest] = typeof(JoinGroupResponse);
            _responseTyps[(Int32)ApiKey.HeartbeatRequest] = typeof(HeartbeatResponse);
            _responseTyps[(Int32)ApiKey.LeaveGroupRequest] = typeof(LeaveGroupResponse);
            _responseTyps[(Int32)ApiKey.SyncGroupRequest] = typeof(SyncGroupResponse);
            _responseTyps[(Int32)ApiKey.DescribeGroupsRequest] = typeof(DescribeGroupsResponse);
            _responseTyps[(Int32)ApiKey.ListGroupsRequest] = typeof(ListGroupsResponse);
        }

        public ReponseDispatcher(ApiKey apiKey, IBufferWrapper bufferWrapper) {
            _apiKey = apiKey;
            _bufferWrapper = bufferWrapper;
        }

        public Response ParseResult() {
            var responseTyp = _responseTyps[(Int32)_apiKey];
            if (responseTyp == null) {
                throw new ArgumentOutOfRangeException("apiKey");
            }
            var response = (Response)Activator.CreateInstance(responseTyp);
            response.Read(_bufferWrapper.Buffer);
            return response;
        }

        public void Dispose() {
            _bufferWrapper.Dispose();
        }
    }
}
