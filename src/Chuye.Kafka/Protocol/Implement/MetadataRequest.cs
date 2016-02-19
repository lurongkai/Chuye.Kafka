using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement {
    //TopicMetadataRequest => [TopicName]
    //  TopicName => string
    public class MetadataRequest : Request {
        public String[] TopicNames { get; set; }

        public MetadataRequest()
            : base(RequestApiKey.MetadataRequest) {
        }

        protected override void SerializeContent(Writer writer) {
            writer.Write(TopicNames.Length);
            foreach (var item in TopicNames) {
                writer.Write(item);
            }
        }
    }

    
}
