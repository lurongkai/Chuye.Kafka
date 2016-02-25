using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //GroupCoordinatorRequest => GroupId
    //  GroupId => string
    public class GroupCoordinatorRequest : Request {
        [Required]
        public String GroupId { get; set; }

        public GroupCoordinatorRequest()
            : base(ApiKey.GroupCoordinatorRequest) {
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(GroupId);
        }
    }
}
