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
            var size = reader.ReadInt32();
            if (size == -1) {
                return;
            }
            TopicPartitions = new OffsetCommitResponseTopicPartition[size];
            for (int i = 0; i < size; i++) {
                TopicPartitions[i] = new OffsetCommitResponseTopicPartition();
                TopicPartitions[i].FetchFrom(reader);
            }
        }
    }

    public class OffsetCommitResponseTopicPartition : IReadable {
        public String TopicName { get; set; }

        public OffsetCommitResponseTopicPartitionDetail[] Details { get; set; }

        public void FetchFrom(BufferReader reader) {
            TopicName = reader.ReadString();
            var size = reader.ReadInt32();
            if (size == -1) {
                return;
            }

            Details = new OffsetCommitResponseTopicPartitionDetail[size];
            for (int i = 0; i < size; i++) {
                Details[i] = new OffsetCommitResponseTopicPartitionDetail();
                Details[i].FetchFrom(reader);
            }
        }
    }

    public class OffsetCommitResponseTopicPartitionDetail : IReadable {
        public Int32 Partition { get; set; }
        public ErrorCode ErrorCode { get; set; }

        public void FetchFrom(BufferReader reader) {
            Partition = reader.ReadInt32();
            ErrorCode = (ErrorCode)reader.ReadInt16();
        }
    }
}
