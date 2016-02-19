using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;

namespace Chuye.Kafka {
    public class Producer {
        private readonly Option _option;
        public Producer(Option option) {
            _option = option;
        }

        public void Post(String topicName, String key, String message) {
            Post(topicName, new KeyedMessage(key, message));
        }

        public void Post(String topicName, KeyedMessage message) {
            Post(topicName, new[] { message });
        }

        public void Post(String topicName, IList<KeyedMessage> messages) {
            var request = new ProduceRequest();
            request.RequiredAcks = 1;
            request.Timeout = 100;
            request.TopicPartitions = new ProduceRequestTopicPartition[1];
            var topicPartition
                = request.TopicPartitions[0]
                = new ProduceRequestTopicPartition();
            topicPartition.TopicName = topicName;
            topicPartition.Details = new ProduceRequestTopicDetail[1];
            var topicDetail
                = topicPartition.Details[0]
                = new ProduceRequestTopicDetail();
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

            var client = new Client(_option);
            var response = client.Send(request);
        }
    }
}