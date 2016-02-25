using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement {

    /// <summary>
    /// ApiVersion 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
    /// </summary>
    public abstract class OffsetCommitRequest : Request {
        protected OffsetCommitRequest()
            : base(ApiKey.OffsetCommitRequest) {
        }

        public static OffsetCommitRequest Create(Int16 version) {
            if (version == 0) {
                return new OffsetCommitRequestV0();
            }
            else if (version == 1) {
                return new OffsetCommitRequestV1();
            }
            else if (version == 2) {
                return new OffsetCommitRequestV2();
            }
            throw new ArgumentOutOfRangeException("version");
        }
    }

    //v0 (supported in 0.8.1 or later)
    //OffsetCommitRequest => ConsumerGroupId [TopicName [Partition Offset Metadata]]
    //  ConsumerGroupId => string
    //  TopicName => string
    //  Partition => int32
    //  Offset => int64
    //  Metadata => string
    public class OffsetCommitRequestV0 : OffsetCommitRequest {
        public String ConsumerGroup { get; set; }

        public OffsetCommitRequestTopicPartitionV0[] TopicPartitions { get; set; }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(ConsumerGroup);
            writer.Write(TopicPartitions.Length);
            for (int i = 0; i < TopicPartitions.Length; i++) {
                TopicPartitions[i].SaveTo(writer);
            }
        }
    }

    //v1 (supported in 0.8.2 or later)
    //OffsetCommitRequest => ConsumerGroupId ConsumerGroupGenerationId ConsumerId [TopicName [Partition Offset TimeStamp Metadata]]
    //  ConsumerGroupId => string
    //  ConsumerGroupGenerationId => int32
    //  ConsumerId => string
    //  TopicName => string
    //  Partition => int32
    //  Offset => int64
    //  TimeStamp => int64
    //  Metadata => string
    public class OffsetCommitRequestV1 : OffsetCommitRequest {
        public String ConsumerGroup { get; set; }
        public Int32 ConsumerGroupGenerationId { get; set; }
        public String ConsumerId { get; set; }
        public OffsetCommitRequestTopicPartitionV1[] TopicPartitions { get; set; }

        protected override void SerializeContent(BufferWriter writer) {
            throw new NotImplementedException();
        }
    }

    //v2 (supported in 0.8.3 or later)
    //OffsetCommitRequest => ConsumerGroup ConsumerGroupGenerationId ConsumerId RetentionTime [TopicName [Partition Offset Metadata]]
    //  ConsumerGroupId => string
    //  ConsumerGroupGenerationId => int32
    //  ConsumerId => string
    //  RetentionTime => int64
    //  TopicName => string
    //  Partition => int32
    //  Offset => int64
    //  Metadata => string
    public class OffsetCommitRequestV2 : OffsetCommitRequest {
        public String ConsumerGroup { get; set; }
        public Int32 ConsumerGroupGenerationId { get; set; }
        public String ConsumerId { get; set; }
        public Int64 RetentionTime { get; set; }
        public OffsetCommitRequestTopicPartitionV0[] TopicPartitions { get; set; }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(ConsumerGroup);
            writer.Write(ConsumerGroupGenerationId);
            writer.Write(ConsumerId);
            writer.Write(RetentionTime);

            writer.Write(TopicPartitions.Length);
            for (int i = 0; i < TopicPartitions.Length; i++) {
                TopicPartitions[i].SaveTo(writer);
            }
        }
    }

    public class OffsetCommitRequestTopicPartitionV0 : IWriteable {
        public String TopicName { get; set; }
        public OffsetCommitRequestTopicPartitionDetailV0[] Details { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(TopicName);
            writer.Write(Details.Length);
            for (int i = 0; i < Details.Length; i++) {
                Details[i].SaveTo(writer);
            }
        }
    }

    public class OffsetCommitRequestTopicPartitionV1 : IWriteable {
        public String TopicName { get; set; }
        public OffsetCommitRequestTopicPartitionDetailV1[] Details { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(TopicName);
            writer.Write(Details.Length);
            for (int i = 0; i < Details.Length; i++) {
                Details[i].SaveTo(writer);
            }
        }
    }

    public class OffsetCommitRequestTopicPartitionDetailV0 : IWriteable {
        public Int32 Partition { get; set; }
        public Int64 Offset { get; set; }
        public String Metadata { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(Partition);
            writer.Write(Offset);
            writer.Write(Metadata);
        }
    }

    public class OffsetCommitRequestTopicPartitionDetailV1 : IWriteable {
        public Int32 Partition { get; set; }
        public Int64 Offset { get; set; }
        public Int64 TimeStamp { get; set; }
        public String Metadata { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(Partition);
            writer.Write(Offset);
            writer.Write(TimeStamp);
            writer.Write(Metadata);
        }
    }
}
