using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;

namespace Chuye.Kafka {
    public class Consumer {
        private readonly Option _option;
        public Consumer(Option option) {
            _option = option;
        }

        public MetadataResponse FetchMetadata(String topicName) {
            var request = new MetadataRequest();
            request.TopicNames = new[] { topicName };

            var client = new Client(_option);
            using (var responseDispatcher = client.Send(request)) {
                var response = (MetadataResponse)responseDispatcher.ParseResult();
                if (response.TopicMetadatas[0].TopicErrorCode != ErrorCode.NoError) {
                    throw new KafkaException(response.TopicMetadatas[0].TopicErrorCode);
                }
                return response;
            }
        }

        public Int64 FetchOffset(String topicName) {
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

            var client = new Client(_option);
            using (var responseDispatcher = client.Send(request)) {
                var response = (OffsetResponse)responseDispatcher.ParseResult();
                var errors = response.TopicPartitions.SelectMany(x => x.Offsets)
                    .Where(x => x.ErrorCode != x.ErrorCode);
                if (errors.Any()) {
                    throw new KafkaException(errors.First().ErrorCode);
                }

                return response.TopicPartitions[0].Offsets[0].Offsets[0];
            }
        }

        public IEnumerable<KeyedMessage> Fetch(String topicName, Int64 fetchOffset) {
            var request = new FetchRequest();
            request.ReplicaId = -1;
            //e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k 
            // would allow the server to wait up to 100ms  
            // to try to accumulate 64k of data before responding
            request.MaxWaitTime = 100;
            request.MinBytes = 64 * 1024;
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
            fetchOffsetDetail.MaxBytes = 64 * 1024;

            var client = new Client(_option);
            using (var responseDispatcher = client.Send(request)) {
                var response = (FetchResponse)responseDispatcher.ParseResult();
                return response.TopicPartitions.SelectMany(x => x.MessageBodys)
                    .SelectMany(x => x.MessageSets.Items)
                    .Select(x => x.Message)
                    .Select(x => {
                        var key = x.Key != null ? Encoding.UTF8.GetString(x.Key) : null;
                        var message = x.Value != null ? Encoding.UTF8.GetString(x.Value) : null;
                        return new KeyedMessage(key, message);
                    });
            }
        }
    }
}
