using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement.Management {
    public class LeaveGroupRequest : Request {
        public LeaveGroupRequest()
            : base(RequestApiKey.LeaveGroupRequest) {

        }

        protected override void SerializeContent(Writer writer) {
            throw new NotImplementedException();
        }
    }
}
