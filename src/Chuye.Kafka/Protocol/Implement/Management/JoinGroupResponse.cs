using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //JoinGroupResponse => ErrorCode GenerationId GroupProtocol LeaderId MemberId Members
    //  ErrorCode => int16
    //  GenerationId => int32
    //  GroupProtocol => string
    //  LeaderId => string
    //  MemberId => string
    //  Members => [MemberId MemberMetadata]
    //    MemberId => string
    //    MemberMetadata => byte
    public class JoinGroupResponse : Response {
        //Possible Error Codes:
        //* GROUP_LOAD_IN_PROGRESS (14)
        //* GROUP_COORDINATOR_NOT_AVAILABLE (15)
        //* NOT_COORDINATOR_FOR_GROUP (16)
        //* INCONSISTENT_GROUP_PROTOCOL (23)
        //* UNKNOWN_MEMBER_ID (25)
        //* INVALID_SESSION_TIMEOUT (26)
        //* GROUP_AUTHORIZATION_FAILED (30)
        public ErrorCode ErrorCode { get; set; }
        public Int32 GenerationId { get; set; }
        public String GroupProtocol { get; set; }
        public String LeaderId { get; set; }
        public String MemberId { get; set; }
        public JoinGroupResponseMember[] Members { get; set; }

        protected override void DeserializeContent(BufferReader reader) {
            ErrorCode     = (ErrorCode)reader.ReadInt16();
            GenerationId  = reader.ReadInt32();
            GroupProtocol = reader.ReadString();
            LeaderId      = reader.ReadString();
            MemberId      = reader.ReadString();

            var size = reader.ReadInt32();
            if (size != -1) {
                Members = new JoinGroupResponseMember[size];
            }
            for (int i = 0; i < size; i++) {
                Members[i] = new JoinGroupResponseMember();
                Members[i].FetchFrom(reader);
            }
        }
    }

    public class JoinGroupResponseMember : IReadable {
        public String MemberId { get; set; }
        public Byte[] MemberMetadata { get; set; }

        public void FetchFrom(BufferReader reader) {
            MemberId = reader.ReadString();
            MemberMetadata = reader.ReadBytes();
        }
    }
}
