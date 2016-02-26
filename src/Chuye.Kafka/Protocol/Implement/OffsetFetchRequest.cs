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

        /// <summary>
        /// ApiVersion 1 and above fetch from Kafka, version 0 fetches from ZooKeeper
        /// </summary>
        public OffsetFetchRequest()
            : base(ApiKey.OffsetFetchRequest) {
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(ConsumerGroup);
            writer.Write(TopicPartitions);
        }

        protected override void DeserializeContent(BufferReader reader) {
            ConsumerGroup   = reader.ReadString();
            TopicPartitions = reader.ReadArray<OffsetFetchRequestTopicPartition>();
        }
    }

    public class OffsetFetchRequestTopicPartition : IWriteable, IReadable {
        public String TopicName { get; set; }
        public Int32[] Partitions { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(TopicName);
            writer.Write(Partitions);
        }

        public void FetchFrom(BufferReader reader) {
            TopicName  = reader.ReadString();
            Partitions = reader.ReadInt32Array();
        }
    }
}
