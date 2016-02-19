using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement.Management {
    public class GroupCoordinatorRequest : Request {
        public GroupCoordinatorRequest() 
            : base(RequestApiKey.GroupCoordinatorRequest) {
        }

        protected override void SerializeContent(Writer writer) {
            throw new NotImplementedException();
        }
    }
}
