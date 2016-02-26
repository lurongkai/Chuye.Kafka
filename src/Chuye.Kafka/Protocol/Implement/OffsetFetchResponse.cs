using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement {
    //OffsetFetchResponse => [TopicName [Partition Offset Metadata ErrorCode]]
    //  TopicName => string
    //  Partition => int32
    //  Offset => int64
    //  Metadata => string
    //  ErrorCode => int16
    public class OffsetFetchResponse : Response {
        public OffsetFetchResponseTopicPartition[] TopicPartitions { get; set; }

        protected override void DeserializeContent(BufferReader reader) {
            TopicPartitions = reader.ReadArray<OffsetFetchResponseTopicPartition>();
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(TopicPartitions);
        }
    }

    public class OffsetFetchResponseTopicPartition : IReadable, IWriteable {
        public String TopicName { get; set; }
        public OffsetFetchResponseTopicPartitionDetail[] Details { get; set; }

        public void FetchFrom(BufferReader reader) {
            TopicName = reader.ReadString();
            Details   = reader.ReadArray<OffsetFetchResponseTopicPartitionDetail>();
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write(TopicName);
            writer.Write(Details);
        }
    }

    public class OffsetFetchResponseTopicPartitionDetail : IReadable, IWriteable {
        public Int32 Partition { get; set; }
        public Int64 Offset { get; set; }
        public String Metadata { get; set; }
        //Possible Error Codes
        //* UNKNOWN_TOPIC_OR_PARTITION (3) <- only for request v0
        //* GROUP_LOAD_IN_PROGRESS (14)
        //* NOT_COORDINATOR_FOR_GROUP (16)
        //* ILLEGAL_GENERATION (22)
        //* UNKNOWN_MEMBER_ID (25)
        //* TOPIC_AUTHORIZATION_FAILED (29)
        //* GROUP_AUTHORIZATION_FAILED (30)
        public ErrorCode ErrorCode { get; set; }

        public void FetchFrom(BufferReader reader) {
            Partition = reader.ReadInt32();
            Offset    = reader.ReadInt64();
            Metadata  = reader.ReadString();
            ErrorCode = (ErrorCode)reader.ReadInt16();
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write(Partition);
            writer.Write(Offset);
            writer.Write(Metadata);
            writer.Write((Int16)ErrorCode);
        }
    }
}
