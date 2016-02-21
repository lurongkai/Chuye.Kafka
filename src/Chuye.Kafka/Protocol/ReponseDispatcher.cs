using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol.Implement;
using Chuye.Kafka.Protocol.Implement.Management;

namespace Chuye.Kafka.Protocol {
    public interface IResponseDispatcher : IDisposable {
        Response ParseResult();
    }

    internal class ReponseDispatcher : IResponseDispatcher {
        private static readonly Type[] _responseTyps;
        private readonly ApiKey _apiKey;
        private readonly IBufferWrapper _bufferWrapper;

        static ReponseDispatcher() {
            _responseTyps = new Type[17];
            _responseTyps[(Int32)ApiKey.ProduceRequest]          = typeof(ProduceResponse);
            _responseTyps[(Int32)ApiKey.FetchRequest]            = typeof(FetchResponse);
            _responseTyps[(Int32)ApiKey.OffsetRequest]           = typeof(OffsetResponse);
            _responseTyps[(Int32)ApiKey.MetadataRequest]         = typeof(MetadataResponse);
            _responseTyps[(Int32)ApiKey.OffsetCommitRequest]     = typeof(OffsetCommitResponse);
            _responseTyps[(Int32)ApiKey.OffsetFetchRequest]      = typeof(OffsetFetchResponse);
            _responseTyps[(Int32)ApiKey.GroupCoordinatorRequest] = typeof(GroupCoordinatorResponse);
            _responseTyps[(Int32)ApiKey.JoinGroupRequest]        = typeof(JoinGroupResponse);
            _responseTyps[(Int32)ApiKey.HeartbeatRequest]        = typeof(HeartbeatResponse);
            _responseTyps[(Int32)ApiKey.LeaveGroupRequest]       = typeof(LeaveGroupResponse);
            _responseTyps[(Int32)ApiKey.SyncGroupRequest]        = typeof(SyncGroupResponse);
            _responseTyps[(Int32)ApiKey.DescribeGroupsRequest]   = typeof(DescribeGroupsResponse);
            _responseTyps[(Int32)ApiKey.ListGroupsRequest]       = typeof(ListGroupsResponse);
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
            response.Read(_bufferWrapper.Segment);
            return response;
        }

        public void Dispose() {
            _bufferWrapper.Dispose();
        }
    }
}
