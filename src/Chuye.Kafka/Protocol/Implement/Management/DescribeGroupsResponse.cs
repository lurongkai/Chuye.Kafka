using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //DescribeGroupsResponse => [ErrorCode GroupId State ProtocolType Protocol Members]
    //  ErrorCode => int16
    //  GroupId => string
    //  State => string
    //  ProtocolType => string
    //  Protocol => string
    //  Members => [MemberId ClientId ClientHost MemberMetadata MemberAssignment]
    //    MemberId => string
    //    ClientId => string
    //    ClientHost => string
    //    MemberMetadata => bytes
    //    MemberAssignment => bytes
    public class DescribeGroupsResponse : Response {
        public DescribeGroupsResponseDetail[] Details { get; set; }

        protected override void DeserializeContent(BufferReader reader) {
            var size = reader.ReadInt32();
            if (size == -1) {
                return;
            }
            Details = new DescribeGroupsResponseDetail[size];
            for (int i = 0; i < size; i++) {
                Details[i] = new DescribeGroupsResponseDetail();
                Details[i].FetchFrom(reader);
            }
        }
    }

    public class DescribeGroupsResponseDetail : IReadable {
        /// <summary>
        /// Possible Error Codes:
        ///  GROUP_LOAD_IN_PROGRESS (14)
        ///  GROUP_COORDINATOR_NOT_AVAILABLE (15)
        ///  NOT_COORDINATOR_FOR_GROUP (16)
        ///  AUTHORIZATION_FAILED (29)
        /// </summary>
        public ErrorCode ErrorCode { get; set; }
        public String GroupId { get; set; }
        public String State { get; set; }
        public String ProtocolType { get; set; }
        public String Protocol { get; set; }
        public DescribeGroupsResponseMember[] Members { get; set; }

        public void FetchFrom(BufferReader reader) {
            ErrorCode = (ErrorCode)reader.ReadInt16();
            GroupId = reader.ReadString();
            State = reader.ReadString();
            ProtocolType = reader.ReadString();
            Protocol = reader.ReadString();

            var size = reader.ReadInt32();
            if (size == -1) {
                return;
            }
            Members = new DescribeGroupsResponseMember[size];
            for (int i = 0; i < size; i++) {
                Members[i] = new DescribeGroupsResponseMember();
                Members[i].FetchFrom(reader);
            }
        }
    }

    public class DescribeGroupsResponseMember : IReadable {
        public String MemberId { get; set; }
        public String ClientId { get; set; }
        public String ClientHost { get; set; }
        public Byte[] MemberMetadata { get; set; }
        public Byte[] MemberAssignment { get; set; }

        public void FetchFrom(BufferReader reader) {
            MemberId = reader.ReadString();
            ClientId = reader.ReadString();
            ClientHost = reader.ReadString();
            MemberMetadata = reader.ReadBytes();
            MemberAssignment = reader.ReadBytes();
        }
    }
}
