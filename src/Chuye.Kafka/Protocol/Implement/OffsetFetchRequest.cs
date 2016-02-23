using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement {
    //OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
    //  ConsumerGroup => string
    //  TopicName => string
    //  Partition => int32
    public class OffsetFetchRequest : Request {
        public String ConsumerGroup { get; set; }
        public OffsetFetchRequestTopicPartition[] TopicPartitions { get; set; }

        public OffsetFetchRequest()
            : base(ApiKey.OffsetFetchRequest) {
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(ConsumerGroup);
            writer.Write(TopicPartitions.Length);
            foreach (var item in TopicPartitions) {
                item.SaveTo(writer);
            }
        }
    }

    public class OffsetFetchRequestTopicPartition : IWriteable {
        public String TopicName { get; set; }
        public Int32[] Partitions { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(TopicName);
            writer.Write(Partitions.Length);
            foreach (var item in Partitions) {
                writer.Write(item);
            }            
        }
    }
}
