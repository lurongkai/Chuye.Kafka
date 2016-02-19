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
        public void Post(String topicName, String key, String value) {
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
            topicDetail.MessageSets.Items = new MessageSet[1];
            var messageSet
                = topicDetail.MessageSets.Items[0]
                = new MessageSet();
            messageSet.Message = new Message();
            messageSet.Message.Key = Encoding.UTF8.GetBytes(key);
            messageSet.Message.Value = Encoding.UTF8.GetBytes(value);

            var response = new Client().Send(request);
        }
    }
}