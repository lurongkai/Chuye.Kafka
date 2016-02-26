using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

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

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write((Int16)RequiredAcks);
            writer.Write(Timeout);
            writer.Write(TopicPartitions);
        }

        protected override void DeserializeContent(BufferReader reader) {
            RequiredAcks    = (AcknowlegeStrategy)reader.ReadInt16();
            Timeout         = reader.ReadInt32();
            TopicPartitions = reader.ReadArray<ProduceRequestTopicPartition>();
        }

        public static ProduceRequest Create(String topicName, IList<KeyedMessage> messages, AcknowlegeStrategy strategy) {
            if (String.IsNullOrWhiteSpace(topicName)) {
                throw new ArgumentOutOfRangeException("topicName");
            }
            if (messages == null || messages.Count == 0) {
                throw new ArgumentOutOfRangeException("messages");
            }

            var messageSetArray = new MessageSet[messages.Count];
            for (int i = 0; i < messageSetArray.Length; i++) {
                var messageSet = messageSetArray[i] = new MessageSet();
                messageSet.Message = new Message();
                if (messages[i].Key != null) {
                    messageSet.Message.Key = Encoding.UTF8.GetBytes(messages[i].Key);
                }
                if (messages[i].Message != null) {
                    messageSet.Message.Value = Encoding.UTF8.GetBytes(messages[i].Message);
                }
            }

            var request = new ProduceRequest();
            request.RequiredAcks = strategy; //important
            request.Timeout = 10;
            request.TopicPartitions = new[] {
                new ProduceRequestTopicPartition {
                    TopicName = topicName,
                    Details =new [] {
                        new ProduceRequestTopicDetail {
                            Partition = 0,
                            MessageSets = new MessageSetCollection {
                                Items = messageSetArray
                            }
                        }
                    }
                }
            };
            return request;
        }

    }

    public class ProduceRequestTopicPartition : IWriteable, IReadable {
        public String TopicName { get; set; }
        public ProduceRequestTopicDetail[] Details { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(TopicName);
            writer.Write(Details);
        }

        public void FetchFrom(BufferReader reader) {
            TopicName = reader.ReadString();
            Details   = reader.ReadArray<ProduceRequestTopicDetail>();
        }
    }

    public class ProduceRequestTopicDetail : IWriteable, IReadable {
        public Int32 Partition { get; set; }
        //public Int32 MessageSetSize { get; set; }
        public MessageSetCollection MessageSets { get; set; }

        public void SaveTo(BufferWriter writer) {
            writer.Write(Partition);
            //writer.Write(MessageSetSize);
            using (writer.PrepareLength()) {
                MessageSets.SaveTo(writer);
            }
        }

        public void FetchFrom(BufferReader reader) {
            Partition          = reader.ReadInt32();
            var messageSetSize = reader.ReadInt32(); //MessageSetSize
            MessageSets        = new MessageSetCollection(messageSetSize);
            MessageSets.FetchFrom(reader);
        }
    }
}
