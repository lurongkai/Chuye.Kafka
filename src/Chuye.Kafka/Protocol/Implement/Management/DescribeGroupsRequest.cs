using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //DescribeGroupsRequest => [GroupId]
    //  GroupId => string
    public class DescribeGroupsRequest : Request {
        [Required]
        public String[] GroupId { get; set; }

        public DescribeGroupsRequest()
            : base(ApiKey.DescribeGroupsRequest) {
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(GroupId.Length);
            foreach (var item in GroupId) {
                writer.Write(item);
            }
        }
    }
}
