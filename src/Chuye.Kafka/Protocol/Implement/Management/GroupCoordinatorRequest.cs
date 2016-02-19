using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //GroupCoordinatorRequest => GroupId
    //  GroupId => string
    public class GroupCoordinatorRequest : Request {
        public String GroupId { get; set; }

        public GroupCoordinatorRequest()
            : base(Protocol.ApiKey.GroupCoordinatorRequest) {
        }

        protected override void SerializeContent(Writer writer) {
            writer.Write(GroupId);
        }
    }
}
