using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //ListGroupsRequest =>
    public class ListGroupsRequest : Request {
        public ListGroupsRequest()
            : base(ApiKey.ListGroupsRequest) {
        }

        protected override void SerializeContent(Writer writer) {
        }
    }
}
