using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement {
    public class FetchRequest : Request {
        public FetchRequest()
            : base(RequestApiKey.FetchRequest) {
        }
        
        protected override void SerializeContent(Writer writer) {
            throw new NotImplementedException();
        }
    }
}
