using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement {
    //FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
    //  ReplicaId => int32
    //  MaxWaitTime => int32
    //  MinBytes => int32
    //  TopicName => string
    //  Partition => int32
    //  FetchOffset => int64
    //  MaxBytes => int32
    public class FetchRequest : Request {
        public Int32 ReplicaId { get; set; }
        public Int32 MaxWaitTime { get; set; }
        public Int32 MinBytes { get; set; }
        public TopicPartition[] TopicPartitions { get; set; }

        public FetchRequest()
            : base(ApiKey.FetchRequest) {
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(ReplicaId);
            writer.Write(MaxWaitTime);
            writer.Write(MinBytes);

            writer.Write(TopicPartitions.Length);
            foreach (var partition in TopicPartitions) {
                partition.SaveTo(writer);
            }
        }
    }

    public class TopicPartition : IWriteable {
        public String TopicName { get; set; }
        public FetchOffsetDetail[] FetchOffsetDetails { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(TopicName);
            if (FetchOffsetDetails == null) {
                writer.Write(0);
                return;
            }

            writer.Write(FetchOffsetDetails.Length);
            foreach (var detail in FetchOffsetDetails) {
                detail.SaveTo(writer);
            }
        }
    }

    public class FetchOffsetDetail : IWriteable {
        public Int32 Partition { get; set; }
        public Int64 FetchOffset { get; set; }
        public Int32 MaxBytes { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(Partition);
            writer.Write(FetchOffset);
            writer.Write(MaxBytes);
        }
    }
}
