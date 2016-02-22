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

        public static ProduceRequest Create(String topicName, IList<KeyedMessage> messages, AcknowlegeStrategy strategy) {
            if (String.IsNullOrWhiteSpace(topicName)) {
                throw new ArgumentOutOfRangeException("topicName");
            }
            if (messages == null || messages.Count == 0) {
                throw new ArgumentOutOfRangeException("messages");
            }

            var request = new ProduceRequest();
            request.RequiredAcks = strategy; //important
            request.Timeout = 10;
            request.TopicPartitions = new ProduceRequestTopicPartition[1];
            var topicPartition
                = request.TopicPartitions[0]
                = new ProduceRequestTopicPartition();
            topicPartition.TopicName = topicName;
            topicPartition.Details = new ProduceRequestTopicDetail[1];
            var topicDetail
                = topicPartition.Details[0]
                = new ProduceRequestTopicDetail();
            topicDetail.Partition = 0; //important, accounding to server config
            topicDetail.MessageSets = new MessageSetCollection();
            topicDetail.MessageSets.Items = new MessageSet[messages.Count];
            for (int i = 0; i < messages.Count; i++) {
                var messageSet
                    = topicDetail.MessageSets.Items[i]
                    = new MessageSet();
                messageSet.Message = new Message();
                if (messages[i].Key != null) {
                    messageSet.Message.Key = Encoding.UTF8.GetBytes(messages[i].Key);
                }
                if (messages[i].Message != null) {
                    messageSet.Message.Value = Encoding.UTF8.GetBytes(messages[i].Message);
                }
            }
            return request;
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
