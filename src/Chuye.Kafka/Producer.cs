using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;

namespace Chuye.Kafka {
    public class Producer : IDisposable {
        private readonly Option _option;
        private readonly Client _client;

        public AcknowlegeStrategy Strategy { get; set; }

        public Producer(Option option) {
            _option = option;
            _client = new Client(option);
        }

        public void Post(String topicName, String key, String message) {
            Post(topicName, new KeyedMessage(key, message));
        }

        public void Post(String topicName, KeyedMessage message) {
            Post(topicName, new[] { message });
        }

        public void Post(String topicName, IList<KeyedMessage> messages) {
            ProduceRequest request = ProduceRequest.Create(topicName, messages, Strategy);
            using (var responseDispatcher = _client.Send(request)) {
                if (request.RequiredAcks == AcknowlegeStrategy.Immediate) {
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

        public Task PostAsync(String topicName, String key, String message) {
            return PostAsync(topicName, new KeyedMessage(key, message));
        }

        public Task PostAsync(String topicName, KeyedMessage message) {
            return PostAsync(topicName, new[] { message });
        }

        public async Task PostAsync(String topicName, IList<KeyedMessage> messages) {
            ProduceRequest request = ProduceRequest.Create(topicName, messages, Strategy);

            using (var responseDispatcher = await _client.SendAsync(request)) {
                if (request.RequiredAcks == AcknowlegeStrategy.Immediate) {
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

        public void Dispose() {
            _client.Dispose();
        }
    }



    /// <summary>
    /// This field indicates how many acknowledgements the servers should receive before responding to the request. 
    ///   If it is  0 the server will not send any response (this is the only case where the server will not reply to a request). 
    ///   If it is  1, the server will wait the data is written to the local log before sending a response. 
    ///   If it is -1 the server will block until the message is committed by all in sync replicas before sending a response. 
    ///   For any number > 1 the server will block waiting for this number of acknowledgements to occur (but the server will never wait for more acknowledgements than there are in-sync replicas).
    /// </summary>
    public enum AcknowlegeStrategy : short  {
        Immediate = 0, Written = 1, Block = -1
    }

    public enum MessageCodec {
        CodecNone = 0x00, CodecGzip = 0x01, CodecSnappy = 0x02
    }
}