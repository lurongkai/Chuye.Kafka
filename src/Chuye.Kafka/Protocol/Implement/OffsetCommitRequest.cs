using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement {
    //OffsetCommitRequest => ConsumerGroup ConsumerGroupGenerationId ConsumerId RetentionTime [TopicName [Partition Offset Metadata]]
    //  ConsumerGroupId => string
    //  ConsumerGroupGenerationId => int32
    //  ConsumerId => string
    //  RetentionTime => int64
    //  TopicName => string
    //  Partition => int32
    //  Offset => int64
    //  Metadata => string
    public class OffsetCommitRequest : Request {
        public String ConsumerGroupId { get; set; }
        public Int32 ConsumerGroupGenerationId { get; set; }
        public String ConsumerId { get; set; }
        public Int64 RetentionTime { get; set; }
        public OffsetCommitRequestTopicPartition[] Partitions { get; set; }

        public OffsetCommitRequest()
            : base(RequestApiKey.OffsetCommitRequest) {
        }

        protected override void SerializeContent(Writer writer) {
            writer.Write(ConsumerGroupId);
            writer.Write(ConsumerGroupGenerationId);
            writer.Write(ConsumerId);
            writer.Write(RetentionTime);

            writer.Write(Partitions.Length);
            for (int i = 0; i < Partitions.Length; i++) {
                Partitions[i].SaveTo(writer);
            }
        }
    }

    public class OffsetCommitRequestTopicPartition : IWriteable {
        public String TopicName { get; set; }
        public OffsetCommitRequestTopicPartitionDetail[] Details { get; set; }

        public void SaveTo(Writer writer) {
            writer.Write(TopicName);
            writer.Write(Details.Length);
            for (int i = 0; i < Details.Length; i++) {
                Details[i].SaveTo(writer);
            }
        }
    }

    public class OffsetCommitRequestTopicPartitionDetail : IWriteable {
        public Int32 Partition { get; set; }
        public Int64 Offset { get; set; }
        public String Metadata { get; set; }

        public void SaveTo(Writer writer) {
            writer.Write(Partition);
            writer.Write(Offset);
            writer.Write(Metadata);
        }
    }
}
