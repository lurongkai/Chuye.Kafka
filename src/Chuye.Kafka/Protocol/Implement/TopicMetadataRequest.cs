using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement {
    //TopicMetadataRequest => [TopicName]
    //  TopicName => string
    public class TopicMetadataRequest : Request {
        public String[] TopicNames { get; set; }

        public TopicMetadataRequest()
            : base(ApiKey.MetadataRequest) {
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(TopicNames.Length);
            foreach (var item in TopicNames) {
                writer.Write(item);
            }
        }
    }

    
}
