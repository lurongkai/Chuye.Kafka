using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement.Management {
    //SyncGroupRequest => GroupId GenerationId MemberId GroupAssignment
    //  GroupId => string
    //  GenerationId => int32
    //  MemberId => string
    //  GroupAssignment => [MemberId MemberAssignment]
    //    MemberId => string
    //    MemberAssignment => bytes
    public class SyncGroupRequest : Request {
        public String GroupId { get; set; }
        public Int32 GenerationId { get; set; }
        public String MemberId { get; set; }
        public SyncGroupRequestGroupAssignment[] GroupAssignments { get; set; }

        public SyncGroupRequest()
            : base(ApiKey.SyncGroupRequest) {
        }

        protected override void SerializeContent(Writer writer) {
            writer.Write(GroupId);
            writer.Write(GenerationId);
            writer.Write(MemberId);

            writer.Write(GroupAssignments.Length);
            foreach (var item in GroupAssignments) {
                item.SaveTo(writer);
            }
        }
    }

    public class SyncGroupRequestGroupAssignment : IWriteable {
        public String MemberId { get; set; }
        public Byte[] MemberAssignment { get; set; }

        public void SaveTo(Writer writer) {
            writer.Write(MemberId);
            writer.Write(MemberAssignment);
        }
    }
}
