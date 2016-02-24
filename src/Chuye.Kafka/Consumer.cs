using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;

namespace Chuye.Kafka {
    public class Consumer : IDisposable {
        private readonly Client _client;

        public Consumer(Option option) {
            _client = new Client(option);
        }

        public TopicMetadataResponse TopicMetadata(String topicName) {
            var request = new TopicMetadataRequest();
            request.TopicNames = new[] { topicName };
            var attemptLimit = 5;
            var response = (TopicMetadataResponse)Invoke(request);
            while (attemptLimit-- > 0) {
                var metadata = response.TopicMetadatas[0];
                if (metadata.TopicErrorCode == ErrorCode.NoError) {
                    break;
                }
                if (metadata.TopicErrorCode == ErrorCode.LeaderNotAvailable) {
                    if (attemptLimit <= 0) {
                        throw new KafkaException(metadata.TopicErrorCode);
                    }
                    response = (TopicMetadataResponse)Invoke(request);
                }
                else {
                    throw new KafkaException(metadata.TopicErrorCode);
                }
            }
            return response;
        }

        private Response Invoke(TopicMetadataRequest request) {
            using (var responseDispatcher = _client.Send(request)) {
                return responseDispatcher.ParseResult();
            }
        }

        public Int64 Offset(String topicName, OffsetTimeOption offsetTime = OffsetTimeOption.Latest) {
            var request = new OffsetRequest();
            request.ReplicaId = 0;
            request.TopicPartitions = new[] {
                 new OffsetsRequestTopicPartition {
                     TopicName = topicName,
                     Details = new [] {
                         new OffsetsRequestTopicPartitionDetail() {
                             Partition = 0,
                             Time = (Int64)offsetTime,
                             MaxNumberOfOffsets = 1
                         }
                     }
                 }
            };

            using (var responseDispatcher = _client.Send(request)) {
                var response = (OffsetResponse)responseDispatcher.ParseResult();
                var errors = response.TopicPartitions.SelectMany(x => x.PartitionOffsets)
                    .Where(x => x.ErrorCode != x.ErrorCode);
                if (errors.Any()) {
                    throw new KafkaException(errors.First().ErrorCode);
                }

                var partitionOffsets = response.TopicPartitions[0].PartitionOffsets[0];
                if (partitionOffsets.Offsets.Length == 0) {
                    return -1;
                }
                return partitionOffsets.Offsets[0];
            }
        }

        public void OffsetCommit(String topicName, String consumerGroup, Int64 offset) {
            var request = new OffsetCommitRequestV0();
            request.ConsumerGroup = consumerGroup;
            request.TopicPartitions = new OffsetCommitRequestTopicPartitionV0[1];
            request.TopicPartitions = new[] {
                new OffsetCommitRequestTopicPartitionV0 {
                    TopicName = topicName,
                    Details = new [] {
                        new OffsetCommitRequestTopicPartitionDetailV0 {
                            Partition = 0,
                            Offset = offset
                        }
                    }
                }
            };

            using (var responseDispatcher = _client.Send(request)) {
                var offsetCommitResponse = (OffsetCommitResponse)responseDispatcher.ParseResult();
                var errros = offsetCommitResponse.TopicPartitions
                    .SelectMany(r => r.Details)
                    .Where(x => x.ErrorCode != ErrorCode.NoError);
                if (errros.Any()) {
                    throw new KafkaException(errros.First().ErrorCode);
                }
            }
        }

        public Int64 OffsetFetch(String topicName, String consumerGroup) {
            var request = new OffsetFetchRequest();
            request.ConsumerGroup = consumerGroup;
            request.TopicPartitions = new[] {
                new OffsetFetchRequestTopicPartition {
                    TopicName = topicName,
                    Partitions = new[] { 0 }
                }
            };

            using (var responseDispatcher = _client.Send(request)) {
                var offsetFetchResponse = (OffsetFetchResponse)responseDispatcher.ParseResult();
                var errros = offsetFetchResponse.TopicPartitions
                    .SelectMany(r => r.Details)
                    .Where(x => x.ErrorCode != ErrorCode.NoError);
                if (errros.Any()) {
                    throw new KafkaException(errros.First().ErrorCode);
                }

                return offsetFetchResponse.TopicPartitions[0].Details[0].Offset;
            }
        }

        public IEnumerable<OffsetKeyedMessage> Fetch(String topicName, Int64 fetchOffset) {
            var request = new FetchRequest();
            request.ReplicaId = -1;
            request.MaxWaitTime = 100;
            request.MinBytes = 4096;
            request.TopicPartitions = new[] {
                new TopicPartition {
                    TopicName = topicName,
                    FetchOffsetDetails=new [] {
                        new FetchOffsetDetail {
                            FetchOffset = fetchOffset,
                            MaxBytes = 60 * 1024
                        }
                    }
                }
            };

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

        public IEnumerable<OffsetKeyedMessage> FetchAll(String topicName, Int64 fetchOffset) {
            var messages = Fetch(topicName, fetchOffset);
            while (messages.Any()) {
                foreach (var msg in messages) {
                    fetchOffset = msg.Offset;
                    yield return msg;
                }
                fetchOffset++;
                messages = Fetch(topicName, fetchOffset);
            }
        }

        public void Dispose() {
            _client.Dispose();
        }
    }

    public enum OffsetTimeOption : long {
        Earliest = -2, Latest = -1
    }
}
