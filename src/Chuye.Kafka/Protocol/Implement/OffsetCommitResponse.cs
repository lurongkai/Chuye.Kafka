using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement {
    //v0, v1 and v2:
    //OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
    //  TopicName => string
    //  Partition => int32
    //  ErrorCode => int16
    public class OffsetCommitResponse : Response {
        public OffsetCommitResponseTopicPartition[] TopicPartitions { get; set; }

        protected override void DeserializeContent(BufferReader reader) {
            TopicPartitions = reader.ReadArray<OffsetCommitResponseTopicPartition>();
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(TopicPartitions);
        }
    }

    public class OffsetCommitResponseTopicPartition : IReadable, IWriteable {
        public String TopicName { get; set; }

        public OffsetCommitResponseTopicPartitionDetail[] Details { get; set; }

        public void FetchFrom(BufferReader reader) {
            TopicName = reader.ReadString();
            Details   = reader.ReadArray<OffsetCommitResponseTopicPartitionDetail>();
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write(TopicName);
            writer.Write(Details);
        }
    }

    public class OffsetCommitResponseTopicPartitionDetail : IReadable, IWriteable {
        public Int32 Partition { get; set; }
        public ErrorCode ErrorCode { get; set; }

        public void FetchFrom(BufferReader reader) {
            Partition = reader.ReadInt32();
            ErrorCode = (ErrorCode)reader.ReadInt16();
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write(Partition);
            writer.Write((Int16)ErrorCode);
        }
    }
}
