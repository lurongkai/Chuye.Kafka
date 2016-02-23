using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //LeaveGroupRequest => GroupId MemberId
    //  GroupId => string
    //  MemberId => string
    public class LeaveGroupRequest : Request {
        public String GroupId { get; set; }
        public String MemberId { get; set; }

        public LeaveGroupRequest()
            : base(ApiKey.LeaveGroupRequest) {
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(GroupId);
            writer.Write(MemberId);
        }
    }
}
