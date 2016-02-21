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
        private readonly Client _client;

        public AcknowlegeStrategy Strategy { get; set; }

        public Producer(Option option) {
            _client = new Client(option);
        }

        public void Post(String topicName, String key, String message) {
            Post(topicName, new KeyedMessage(key, message));
        }

        public void Post(String topicName, KeyedMessage message) {
            Post(topicName, new[] { message });
        }

        public void Post(String topicName, IList<KeyedMessage> messages) {
            if (String.IsNullOrWhiteSpace(topicName)) {
                throw new ArgumentOutOfRangeException("topicName");
            }
            if (messages == null || messages.Count == 0) {
                throw new ArgumentOutOfRangeException("messages");
            }

            var request = new ProduceRequest();
            request.RequiredAcks = Strategy; //important
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
                        
            using (var responseDispatcher = _client.Send(request)) {
                if (request.RequiredAcks == AcknowlegeStrategy.Async) {
                    return;
                }

                var response = (ProduceResponse)responseDispatcher.ParseResult();
                var errors = response.TopicPartitions.SelectMany(x => x.Offsets)
                    .Where(x => x.ErrorCode != ErrorCode.NoError);
                if (errors.Any()) {
                    throw new KafkaException(errors.First().ErrorCode);
                }
            };
        }
    }


    /// <summary>
    /// This field indicates how many acknowledgements the servers should receive before responding to the request. 
    ///   If it is  0 the server will not send any response (this is the only case where the server will not reply to a request). 
    ///   If it is  1, the server will wait the data is written to the local log before sending a response. 
    ///   If it is -1 the server will block until the message is committed by all in sync replicas before sending a response. 
    ///   For any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server will never wait for more acknowledgements than there are in-sync replicas).
    /// </summary>
    public enum AcknowlegeStrategy : Int16 {
        Async = 0, Written = 1, Block = -1
    }
}