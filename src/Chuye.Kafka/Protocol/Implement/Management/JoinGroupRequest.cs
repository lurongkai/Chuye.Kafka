using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //JoinGroupRequest => GroupId SessionTimeout MemberId ProtocolType GroupProtocols
    //  GroupId => string
    //  SessionTimeout => int32
    //  MemberId => string
    //  ProtocolType => string
    //  GroupProtocols => [ProtocolName ProtocolMetadata]
    //    ProtocolName => string
    //    ProtocolMetadata => bytes
    public class JoinGroupRequest : Request {
        public String GroupId { get; set; }
        public Int32 SessionTimeout { get; set; }
        public String MemberId { get; set; }
        public String ProtocolType { get; set; }
        public JoinGroupRequestGroupProtocol[] GroupProtocols { get; set; }

        public JoinGroupRequest()
            : base(ApiKey.JoinGroupRequest) {
        }

        protected override void SerializeContent(Writer writer) {
            writer.Write(GroupId);
            writer.Write(SessionTimeout);
            writer.Write(MemberId);
            writer.Write(ProtocolType);

            writer.Write(GroupProtocols.Length);
            foreach (var item in GroupProtocols) {
                item.SaveTo(writer);
            }
        }
    }

    public class JoinGroupRequestGroupProtocol : IWriteable {
        public String ProtocolName { get; set; }
        public Byte[] ProtocolMetadata { get; set; }

        public void SaveTo(Writer writer) {
            writer.Write(ProtocolName);
            writer.Write(ProtocolMetadata);
        }
    }
}
