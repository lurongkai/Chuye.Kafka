using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

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

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(GroupId);
            writer.Write(GenerationId);
            writer.Write(MemberId);
            writer.Write(GroupAssignments);
        }

        protected override void DeserializeContent(BufferReader reader) {
            GroupId          = reader.ReadString();
            GenerationId     = reader.ReadInt32();
            MemberId         = reader.ReadString();
            GroupAssignments = reader.ReadArray<SyncGroupRequestGroupAssignment>();
        }
    }

    public class SyncGroupRequestGroupAssignment : IWriteable, IReadable {
        public String MemberId { get; set; }
        public Byte[] MemberAssignment { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(MemberId);
            writer.Write(MemberAssignment);
        }

        public void FetchFrom(BufferReader reader) {
            MemberId         = reader.ReadString();
            MemberAssignment = reader.ReadBytes();
        }
    }
}
