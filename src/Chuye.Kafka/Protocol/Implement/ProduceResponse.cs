using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement {
    //ProduceResponse => [TopicName [Partition ErrorCode Offset]]
    //  TopicName => string
    //  Partition => int32
    //  ErrorCode => int16
    //  Offset => int64
    public class ProduceResponse : Response {
        public ProduceResponseTopicPartition[] TopicPartitions { get; set; }

        protected override void DeserializeContent(BufferReader reader) {
            TopicPartitions = reader.ReadArray<ProduceResponseTopicPartition>();
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(TopicPartitions);
        }
    }

    public class ProduceResponseTopicPartition : IReadable, IWriteable {
        public String TopicName { get; set; }
        public ProduceResponseTopicPartitionDetail[] Details { get; set; }

        public void FetchFrom(BufferReader reader) {
            TopicName = reader.ReadString();
            Details   = reader.ReadArray<ProduceResponseTopicPartitionDetail>();
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write(TopicName);
            writer.Write(Details);
        }
    }

    public class ProduceResponseTopicPartitionDetail : IReadable, IWriteable {
        public Int32 Partition { get; set; }
        //Possible Error Codes: (TODO)
        public ErrorCode ErrorCode { get; set; }
        public Int64 Offset { get; set; }

        public void FetchFrom(BufferReader reader) {
            Partition = reader.ReadInt32();
            ErrorCode = (ErrorCode)reader.ReadInt16();
            Offset    = reader.ReadInt64();
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write(Partition);
            writer.Write((Int16)ErrorCode);
            writer.Write(Offset);
        }
    }
}
