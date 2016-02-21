using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement {
    //ProduceResponse => [TopicName [Partition ErrorCode Offset]]
    //  TopicName => string
    //  Partition => int32
    //  ErrorCode => int16
    //  Offset => int64
    public class ProduceResponse : Response {
        public ProduceResponseTopicPartition[] TopicPartitions { get; set; }

        protected override void DeserializeContent(Reader reader) {
            TopicPartitions = new ProduceResponseTopicPartition[reader.ReadInt32()];
            for (int i = 0; i < TopicPartitions.Length; i++) {
                TopicPartitions[i] = new ProduceResponseTopicPartition();
                TopicPartitions[i].FetchFrom(reader);
            }
        }
    }

    public class ProduceResponseTopicPartition : IReadable {
        public String TopicName { get; set; }
        public ProduceResponseTopicPartitionDetail[] Offsets { get; set; }

        public void FetchFrom(Reader reader) {
            TopicName = reader.ReadString();
            Offsets = new ProduceResponseTopicPartitionDetail[reader.ReadInt32()];
            for (int i = 0; i < Offsets.Length; i++) {
                Offsets[i] = new ProduceResponseTopicPartitionDetail();
                Offsets[i].FetchFrom(reader);
            }
        }
    }

    public class ProduceResponseTopicPartitionDetail : IReadable {
        public Int32 Partition { get; set; }
        //Possible Error Codes: (TODO)
        public ErrorCode ErrorCode { get; set; }
        public Int64 Offset { get; set; }

        public void FetchFrom(Reader reader) {
            Partition = reader.ReadInt32();
            ErrorCode = (ErrorCode)reader.ReadInt16();
            Offset = reader.ReadInt64();
        }
    }
}
