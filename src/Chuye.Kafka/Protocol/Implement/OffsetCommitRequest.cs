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
            writer.Write(TopicPartitions);
        }

        protected override void DeserializeContent(BufferReader reader) {
            ConsumerGroup   = reader.ReadString();
            TopicPartitions = reader.ReadArray<OffsetCommitRequestTopicPartitionV0>();
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
            writer.Write(ConsumerGroup);
            writer.Write(ConsumerGroupGenerationId);
            writer.Write(ConsumerId);
            writer.Write(TopicPartitions);
        }

        protected override void DeserializeContent(BufferReader reader) {
            ConsumerGroup             = reader.ReadString();
            ConsumerGroupGenerationId = reader.ReadInt32();
            ConsumerId                = reader.ReadString();
            TopicPartitions           = reader.ReadArray<OffsetCommitRequestTopicPartitionV1>();
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
            writer.Write(TopicPartitions);
        }

        protected override void DeserializeContent(BufferReader reader) {
            ConsumerGroup             = reader.ReadString();
            ConsumerGroupGenerationId = reader.ReadInt32();
            ConsumerId                = reader.ReadString();
            RetentionTime             = reader.ReadInt64();
            TopicPartitions           = reader.ReadArray<OffsetCommitRequestTopicPartitionV0>();
        }
    }

    public class OffsetCommitRequestTopicPartitionV0 : IWriteable, IReadable {
        public String TopicName { get; set; }
        public OffsetCommitRequestTopicPartitionDetailV0[] Details { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(TopicName);
            writer.Write(Details);
        }

        public void FetchFrom(BufferReader reader) {
            TopicName = reader.ReadString();
            Details   = reader.ReadArray<OffsetCommitRequestTopicPartitionDetailV0>();
        }
    }

    public class OffsetCommitRequestTopicPartitionV1 : IWriteable, IReadable {
        public String TopicName { get; set; }
        public OffsetCommitRequestTopicPartitionDetailV1[] Details { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(TopicName);
            writer.Write(Details);
        }

        public void FetchFrom(BufferReader reader) {
            TopicName = reader.ReadString();
            Details   = reader.ReadArray<OffsetCommitRequestTopicPartitionDetailV1>();
        }
    }

    public class OffsetCommitRequestTopicPartitionDetailV0 : IWriteable, IReadable {
        public Int32 Partition { get; set; }
        public Int64 Offset { get; set; }
        public String Metadata { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(Partition);
            writer.Write(Offset);
            writer.Write(Metadata);
        }

        public void FetchFrom(BufferReader reader) {
            Partition = reader.ReadInt32();
            Offset    = reader.ReadInt64();
            Metadata  = reader.ReadString();
        }
    }

    public class OffsetCommitRequestTopicPartitionDetailV1 : IWriteable, IReadable {
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

        public void FetchFrom(BufferReader reader) {
            Partition = reader.ReadInt32();
            Offset    = reader.ReadInt64();
            TimeStamp = reader.ReadInt64();
            Metadata  = reader.ReadString();
        }
    }
}
