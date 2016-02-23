using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //HeartbeatRequest => GroupId GenerationId MemberId
    //  GroupId => string
    //  GenerationId => int32
    //  MemberId => string
    public class HeartbeatRequest : Request {
        public String GroupId { get; set; }
        public Int32 GenerationId { get; set; }
        public String MemberId { get; set; }

        public HeartbeatRequest()
            : base(ApiKey.HeartbeatRequest) {
        }

        protected override void SerializeContent(BufferWriter writer) {
            throw new NotImplementedException();
        }
    }
}
