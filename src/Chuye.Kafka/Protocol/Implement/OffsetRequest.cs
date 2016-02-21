using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            : base(Protocol.ApiKey.OffsetRequest) {
        }

        protected override void SerializeContent(Writer writer) {
            writer.Write(ReplicaId);
            writer.Write(TopicPartitions.Length);
            foreach (var item in TopicPartitions) {
                item.SaveTo(writer);
            }
        }
    }

    public class OffsetsRequestTopicPartition : IWriteable {
        public String TopicName { get; set; }
        public OffsetsRequestTopicPartitionDetail[] Details { get; set; }

        public void SaveTo(Writer writer) {
            writer.Write(TopicName);
            writer.Write(Details.Length);
            foreach (var item in Details) {
                item.SaveTo(writer);
            }
        }
    }

    public class OffsetsRequestTopicPartitionDetail : IWriteable {
        public Int32 Partition { get; set; }
        public Int64 Time { get; set; }
        public Int32 MaxNumberOfOffsets { get; set; }

        public void SaveTo(Writer writer) {
            writer.Write(Partition);
            writer.Write(Time);
            writer.Write(MaxNumberOfOffsets);
        }
    }
}
