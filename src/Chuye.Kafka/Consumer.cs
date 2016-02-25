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
        private const Int32 DefaultPartition = 0;
        private const Int32 DefaultReplicaId = -1;

        private readonly Connection _connection;

        public Consumer(Connection connection) {
            _connection = connection;
        }

        public Int64 Offset(String topicName, OffsetTimeOption offsetTime = OffsetTimeOption.Latest) {
            var request = new OffsetRequest();
            request.ReplicaId       = DefaultReplicaId;
            request.TopicPartitions = new[] {
                 new OffsetsRequestTopicPartition {
                     TopicName = topicName,
                     Details   = new [] {
                         new OffsetsRequestTopicPartitionDetail() {
                             Partition          = DefaultPartition,
                             Time               = (Int64)offsetTime,
                             MaxNumberOfOffsets = 1
                         }
                     }
                 }
            };

            var response = (OffsetResponse)_connection.Invoke(request);
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

        public void OffsetCommit(String topicName, String consumerGroup, Int64 offset) {
            var request = new OffsetCommitRequestV0();
            request.ConsumerGroup   = consumerGroup;
            request.TopicPartitions = new OffsetCommitRequestTopicPartitionV0[1];
            request.TopicPartitions = new[] {
                new OffsetCommitRequestTopicPartitionV0 {
                    TopicName = topicName,
                    Details   = new [] {
                        new OffsetCommitRequestTopicPartitionDetailV0 {
                            Partition = DefaultPartition,
                            Offset    = offset
                        }
                    }
                }
            };

            var response = (OffsetCommitResponse)_connection.Invoke(request);
            var errros = response.TopicPartitions
                .SelectMany(r => r.Details)
                .Where(x => x.ErrorCode != ErrorCode.NoError);
            if (errros.Any()) {
                throw new KafkaException(errros.First().ErrorCode);
            }
        }

        public Int64 OffsetFetch(String topicName, String consumerGroup) {
            var request = new OffsetFetchRequest();
            request.ConsumerGroup   = consumerGroup;
            request.TopicPartitions = new[] {
                new OffsetFetchRequestTopicPartition {
                    TopicName  = topicName,
                    Partitions = new[] { DefaultPartition }
                }
            };

            var response = (OffsetFetchResponse)_connection.Invoke(request);
            var errros = response.TopicPartitions
                .SelectMany(r => r.Details)
                .Where(x => x.ErrorCode != ErrorCode.NoError);
            if (errros.Any()) {
                throw new KafkaException(errros.First().ErrorCode);
            }

            return response.TopicPartitions[0].Details[0].Offset;
        }

        public IEnumerable<OffsetKeyedMessage> Fetch(String topicName, Int64 fetchOffset) {
            var request = new FetchRequest();
            request.ReplicaId       = DefaultReplicaId;
            request.MaxWaitTime     = 100;
            request.MinBytes        = 4096;
            request.TopicPartitions = new[] {
                new TopicPartition {
                    TopicName          = topicName,
                    FetchOffsetDetails = new [] {
                        new FetchOffsetDetail {
                            Partition   = 0,
                            FetchOffset = fetchOffset,
                            MaxBytes    = 60 * 1024
                        }
                    }
                }
            };

            var response = (FetchResponse)_connection.Invoke(request);
            var messages = response.TopicPartitions.SelectMany(x => x.MessageBodys)
                .SelectMany(x => x.MessageSet.Items);

            foreach (var msg in messages) {
                var key = msg.Message.Key != null ? Encoding.UTF8.GetString(msg.Message.Key) : null;
                var message = msg.Message.Value != null ? Encoding.UTF8.GetString(msg.Message.Value) : null;
                yield return new OffsetKeyedMessage(msg.Offset, key, message);
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
            _connection.Dispose();
        }
    }

    public enum OffsetTimeOption : long {
        Earliest = -2, Latest = -1
    }
}
