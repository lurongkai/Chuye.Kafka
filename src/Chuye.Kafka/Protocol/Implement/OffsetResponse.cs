using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement {
    //OffsetResponse => [TopicName [PartitionOffsets]]
    //  PartitionOffsets => Partition ErrorCode [Offset]
    //  Partition => int32
    //  ErrorCode => int16
    //  Offset => int64
    public class OffsetResponse : Response {
        public OffsetResponseTopicPartition[] TopicPartitions { get; set; }

        protected override void DeserializeContent(BufferReader reader) {
            var size = reader.ReadInt32();
            if (size == -1) {
                return;
            }

            TopicPartitions = new OffsetResponseTopicPartition[size];
            for (int i = 0; i < size; i++) {
                TopicPartitions[i] = new OffsetResponseTopicPartition();
                TopicPartitions[i].FetchFrom(reader);
            }
        }
    }

    public class OffsetResponseTopicPartition : IReadable {
        public String TopicName { get; set; }
        public PartitionOffset[] PartitionOffsets { get; set; }

        public void FetchFrom(BufferReader reader) {
            TopicName = reader.ReadString();
            var size = reader.ReadInt32();
            if (size == -1) {
                return;
            }

            PartitionOffsets = new PartitionOffset[size];
            for (int i = 0; i < size; i++) {
                PartitionOffsets[i] = new PartitionOffset();
                PartitionOffsets[i].FetchFrom(reader);
            }
        }
    }

    public class PartitionOffset : IReadable {
        public Int32 Partition { get; set; }
        //Possible Error Codes
        //* UNKNOWN_TOPIC_OR_PARTITION (3)
        //* NOT_LEADER_FOR_PARTITION (6)
        //* UNKNOWN (-1)
        public ErrorCode ErrorCode { get; set; }
        public Int64[] Offsets { get; set; }

        public void FetchFrom(BufferReader reader) {
            Partition = reader.ReadInt32();
            ErrorCode = (ErrorCode)reader.ReadInt16();
            var size = reader.ReadInt32();
            if (size == -1) {
                return;
            }
            Offsets = new Int64[size];
            for (int i = 0; i < size; i++) {
                Offsets[i] = reader.ReadInt64();
            }
        }
    }
}
