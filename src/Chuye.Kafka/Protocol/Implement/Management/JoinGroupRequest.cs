using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement.Management {
    public class JoinGroupRequest : Request {
        public JoinGroupRequest()
            : base(RequestApiKey.JoinGroupRequest) {
        }

        protected override void SerializeContent(Writer writer) {
            throw new NotImplementedException();
        }
    }
}
