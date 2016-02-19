using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement {
    public class OffsetCommitRequest : Request {
        public OffsetCommitRequest()
            : base(RequestApiKey.OffsetCommitRequest) {
        }

        protected override void SerializeContent(Writer writer) {
            throw new NotImplementedException();
        }
    }
}
