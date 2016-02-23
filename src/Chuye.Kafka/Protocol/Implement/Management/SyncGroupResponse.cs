using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //SyncGroupResponse => ErrorCode MemberAssignment
    //  ErrorCode => int16
    //  MemberAssignment => bytes
    public class SyncGroupResponse : Response {
        public ErrorCode ErrorCode { get; set; }
        public Byte[] MemberAssignment { get; set; }

        protected override void DeserializeContent(BufferReader reader) {
            //Possible Error Codes:
            //* GROUP_COORDINATOR_NOT_AVAILABLE (15)
            //* NOT_COORDINATOR_FOR_GROUP (16)
            //* ILLEGAL_GENERATION (22)
            //* UNKNOWN_MEMBER_ID (25)
            //* REBALANCE_IN_PROGRESS (27)
            //* GROUP_AUTHORIZATION_FAILED (30)
            ErrorCode = (ErrorCode)reader.ReadInt16();
            MemberAssignment = reader.ReadBytes();
        }
    }
}
