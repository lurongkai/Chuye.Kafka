using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //ListGroupsRequest =>
    public class ListGroupsRequest : Request {
        public ListGroupsRequest()
            : base(ApiKey.ListGroupsRequest) {
        }

        protected override void SerializeContent(BufferWriter writer) {
        }

        protected override void DeserializeContent(BufferReader reader) {
        }
    }
}
