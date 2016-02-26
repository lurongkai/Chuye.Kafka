using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement {
    //OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
    //  ReplicaId => int32
    //  TopicName => string
    //  Partition => int32
    //  Time => int64
    //  MaxNumberOfOffsets => int32
    public class OffsetRequest : Request {
        public Int32 ReplicaId { get; set; }
        public OffsetsRequestTopicPartition[] TopicPartitions { get; set; }

        public OffsetRequest()
            : base(ApiKey.OffsetRequest) {
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(ReplicaId);
            writer.Write(TopicPartitions);
        }

        protected override void DeserializeContent(BufferReader reader) {
            ReplicaId       = reader.ReadInt32();
            TopicPartitions = reader.ReadArray<OffsetsRequestTopicPartition>();
        }
    }

    public class OffsetsRequestTopicPartition : IWriteable, IReadable {
        public String TopicName { get; set; }
        public OffsetsRequestTopicPartitionDetail[] Details { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(TopicName);
            writer.Write(Details);
        }

        public void FetchFrom(BufferReader reader) {
            TopicName = reader.ReadString();
            Details   = reader.ReadArray<OffsetsRequestTopicPartitionDetail>();
        }
    }

    public class OffsetsRequestTopicPartitionDetail : IWriteable, IReadable {
        public Int32 Partition { get; set; }
        /// <summary>
        /// Used to ask for all messages before a certain time (ms). There are two special values. 
        ///   Specify -1 to receive the latest offset (i.e. the offset of the next coming message) 
        ///   and -2 to receive the earliest available offset. 
        ///   Note that because offsets are pulled in descending order, asking for the earliest offset will always return you a single element.
        /// </summary>
        public Int64 Time { get; set; }
        public Int32 MaxNumberOfOffsets { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(Partition);
            writer.Write(Time);
            writer.Write(MaxNumberOfOffsets);
        }

        public void FetchFrom(BufferReader reader) {
            Partition          = reader.ReadInt32();
            Time               = reader.ReadInt64();
            MaxNumberOfOffsets = reader.ReadInt32();
        }
    }
}
