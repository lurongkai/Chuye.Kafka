using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement {
//ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
//  RequiredAcks => int16
//  Timeout => int32
//  Partition => int32
//  MessageSetSize => int32
    public class ProduceRequest : Request {
        public ProduceRequest()
            : base(ApiKey.ProduceRequest) {
        }

        public AcknowlegeStrategy RequiredAcks { get; set; }
        public Int32 Timeout { get; set; }
        public ProduceRequestTopicPartition[] TopicPartitions { get; set; }

        protected override void SerializeContent(Writer writer) {
            writer.Write((Int16)RequiredAcks);
            writer.Write(Timeout);
            writer.Write(TopicPartitions.Length);
            foreach (var item in TopicPartitions) {
                item.SaveTo(writer);
            }
        }
    }

    public class ProduceRequestTopicPartition : IWriteable {
        public String TopicName { get; set; }
        public ProduceRequestTopicDetail[] Details { get; set; }

        public void SaveTo(Writer writer) {
            writer.Write(TopicName);
            writer.Write(Details.Length);
            foreach (var item in Details) {
                item.SaveTo(writer);
            }
        }
    }

    public class ProduceRequestTopicDetail : IWriteable {
        public Int32 Partition { get; set; }
        //public Int32 MessageSetSize { get; set; }
        public MessageSetCollection MessageSets { get; set; }

        public void SaveTo(Writer writer) {
            writer.Write(Partition);
            //writer.Write(MessageSetSize);
            using (writer.PrepareLength()) {
                MessageSets.SaveTo(writer);
            }
        }
    }
}
