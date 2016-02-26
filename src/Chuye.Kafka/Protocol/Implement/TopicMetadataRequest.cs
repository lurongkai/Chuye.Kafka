using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement {
    //TopicMetadataRequest => [TopicName]
    //  TopicName => string
    /// <summary>
    /// If "auto.create.topics.enable" is set in the broker configuration, 
    ///   a topic metadata request will create the topic with the default replication factor and number of partitions. 
    /// </summary>
    public class TopicMetadataRequest : Request {
        public String[] TopicNames { get; set; }

        public TopicMetadataRequest()
            : base(ApiKey.MetadataRequest) {
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(TopicNames);
        }

        protected override void DeserializeContent(BufferReader reader) {
            TopicNames = reader.ReadStrings();
        }
    }
}
