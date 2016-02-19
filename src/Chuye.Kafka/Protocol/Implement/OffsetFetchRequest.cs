using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement {
    public class OffsetFetchRequest : Request {
        public OffsetFetchRequest()
            : base(RequestApiKey.OffsetFetchRequest) {
        }

        protected override void SerializeContent(Writer writer) {
            throw new NotImplementedException();
        }
    }
}
