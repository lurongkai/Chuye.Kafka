using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement.Management {
    public class HeartbeatRequest : Request {
        public HeartbeatRequest()
            : base(RequestApiKey.HeartbeatRequest) {
        }

        protected override void SerializeContent(Writer writer) {
            throw new NotImplementedException();
        }
    }
}
