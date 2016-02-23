using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;

namespace Chuye.Kafka {
    public class Consumer {
        private readonly Client _client;

        public Consumer(Option option) {
            _client = new Client(option);
        }

        public MetadataResponse Metadata(String topicName) {
            var request = new MetadataRequest();
            request.TopicNames = new[] { topicName };

            using (var responseDispatcher = _client.Send(request)) {
                var response = (MetadataResponse)responseDispatcher.ParseResult();
                var errors = response.TopicMetadatas.Where(x => x.TopicErrorCode != ErrorCode.NoError);
                if (errors.Any()) {
                    throw new KafkaException(errors.First().TopicErrorCode);
                }
                return response;
            }
        }

        public Int64 Offset(String topicName) {
            var request = new OffsetRequest();
            request.ReplicaId = 0;
            request.TopicPartitions = new OffsetsRequestTopicPartition[1];
            var partition
                = request.TopicPartitions[0]
                = new OffsetsRequestTopicPartition();
            partition.TopicName = topicName;
            partition.Details = new OffsetsRequestTopicPartitionDetail[1];
            var detail
                = partition.Details[0]
                = new OffsetsRequestTopicPartitionDetail();
            detail.Partition = 0;
            detail.Time = -2;
            detail.MaxNumberOfOffsets = 1;

            using (var responseDispatcher = _client.Send(request)) {
                var response = (OffsetResponse)responseDispatcher.ParseResult();
                var errors = response.TopicPartitions.SelectMany(x => x.Offsets)
                    .Where(x => x.ErrorCode != x.ErrorCode);
                if (errors.Any()) {
                    throw new KafkaException(errors.First().ErrorCode);
                }

                return response.TopicPartitions[0].Offsets[0].Offsets[0];
            }
        }

        public IEnumerable<OffsetKeyedMessage> FetchAll(String topicName, Int64 fetchOffset) {
            //var lastOffset = 0;
            //return Fetch(topicName, fetchOffset).TakeWhile(r => r.Offset == lastOffset);
            //var nextOffset = lastOffset;
            //...

            var messages = Fetch(topicName, fetchOffset);
            while (messages.Any()) {
                foreach (var msg in messages) {
                    //if (msg.Offset == 0) ;
                    fetchOffset = msg.Offset;
                    yield return msg;
                }
                fetchOffset++;
                messages = Fetch(topicName, fetchOffset);
            }
        }

        public IEnumerable<OffsetKeyedMessage> Fetch(String topicName, Int64 fetchOffset) {
            var request = new FetchRequest();
            request.ReplicaId = -1;
            //e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k 
            // would allow the server to wait up to 100ms  
            // to try to accumulate 30k of data before responding
            request.MaxWaitTime = 100;
            request.MinBytes = 4096;
            request.TopicPartitions = new TopicPartition[1];
            var topicPartition
                = request.TopicPartitions[0]
                = new TopicPartition();
            topicPartition.TopicName = topicName;
            topicPartition.FetchOffsetDetails = new FetchOffsetDetail[1];
            var fetchOffsetDetail
                = topicPartition.FetchOffsetDetails[0]
                = new FetchOffsetDetail();
            fetchOffsetDetail.FetchOffset = fetchOffset;
            fetchOffsetDetail.MaxBytes = 60 * 1024;

            using (var responseDispatcher = _client.Send(request)) {
                var response = (FetchResponse)responseDispatcher.ParseResult();
                var messages = response.TopicPartitions.SelectMany(x => x.MessageBodys)
                    .SelectMany(x => x.MessageSet.Items);

                foreach (var msg in messages) {
                    var key = msg.Message.Key != null ? Encoding.UTF8.GetString(msg.Message.Key) : null;
                    var message = msg.Message.Value != null ? Encoding.UTF8.GetString(msg.Message.Value) : null;
                    yield return new OffsetKeyedMessage(msg.Offset, key, message);
                }
            }
        }
    }
}
