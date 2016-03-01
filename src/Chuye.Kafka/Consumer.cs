using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;
using Chuye.Kafka.Protocol.Implement.Management;

namespace Chuye.Kafka {
    public class Consumer : IDisposable {
        private const Int32 DefaultReplicaId = -1;
        private const String DefaultProtocolType = "";
        private const String DefaultProtocolName = "";
        private readonly Connection _connection;

        private String _groupId;
        private String _memberId;
        private Int32 _generationId;

        public String GroupId {
            get { return _groupId; }
        }

        public String MemberId {
            get { return _memberId; }
        }

        public Int32 GenerationId {
            get { return _generationId; }
        }

        public Consumer(Connection connection) {
            _connection = connection;
        }

        public Int64 Offset(String topicName, OffsetTimeOption offsetTime = OffsetTimeOption.Latest) {
            var connection = _connection.Route(topicName);
            var partitionId = connection.CurrentPartition;

            var request = new OffsetRequest();
            request.ReplicaId = DefaultReplicaId;
            request.TopicPartitions = new[] {
                 new OffsetsRequestTopicPartition {
                     TopicName = topicName,
                     Details   = new [] {
                         new OffsetsRequestTopicPartitionDetail() {
                             Partition          = partitionId,
                             Time               = (Int64)offsetTime,
                             MaxNumberOfOffsets = 1
                         }
                     }
                 }
            };

            var response = (OffsetResponse) ((Connection)connection).Invoke(request);
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
            var connection = _connection.Route(topicName);
            var partitionId = connection.CurrentPartition;

            var request = new OffsetCommitRequestV0();
            request.ConsumerGroup = consumerGroup;
            request.TopicPartitions = new OffsetCommitRequestTopicPartitionV0[1];
            request.TopicPartitions = new[] {
                new OffsetCommitRequestTopicPartitionV0 {
                    TopicName = topicName,
                    Details   = new [] {
                        new OffsetCommitRequestTopicPartitionDetailV0 {
                            Partition = partitionId,
                            Offset    = offset
                        }
                    }
                }
            };

            var response = (OffsetCommitResponse)((Connection)connection).Invoke(request);
            var errors = response.TopicPartitions
                .SelectMany(r => r.Details)
                .Where(x => x.ErrorCode != ErrorCode.NoError);
            if (errors.Any()) {
                throw new KafkaException(errors.First().ErrorCode);
            }
        }

        public Int64 OffsetFetch(String topicName, String consumerGroup) {
            var connection = _connection.Route(topicName);
            var partitionId = connection.CurrentPartition;

            var request = new OffsetFetchRequest();
            request.ConsumerGroup = consumerGroup;
            request.TopicPartitions = new[] {
                new OffsetFetchRequestTopicPartition {
                    TopicName  = topicName,
                    Partitions = new[] { partitionId }
                }
            };

            var response = (OffsetFetchResponse)((Connection)connection).Invoke(request);
            var errors = response.TopicPartitions
                .SelectMany(r => r.Details)
                .Where(x => x.ErrorCode != ErrorCode.NoError);
            if (errors.Any()) {
                throw new KafkaException(errors.First().ErrorCode);
            }

            return response.TopicPartitions[0].Details[0].Offset;
        }

        public IEnumerable<OffsetKeyedMessage> Fetch(String topicName, Int64 fetchOffset) {
            var connection = _connection.Route(topicName);
            var partitionId = connection.CurrentPartition;

            var request = new FetchRequest();
            request.ReplicaId = DefaultReplicaId;
            request.MaxWaitTime = 100;
            request.MinBytes = 4096;
            request.TopicPartitions = new[] {
                new FetchRequestTopicPartition {
                    TopicName          = topicName,
                    FetchOffsetDetails = new [] {
                        new FetchRequestTopicPartitionDetail {
                            Partition   = partitionId,
                            FetchOffset = fetchOffset,
                            MaxBytes    = 60 * 1024
                        }
                    }
                }
            };

            var response = (FetchResponse)((Connection)connection).Invoke(request);
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

        public void GroupCoordinator(String groupId) {
            var request = new GroupCoordinatorRequest();
            request.GroupId = groupId;
            var response = (GroupCoordinatorResponse)_connection.Invoke(request);
            if (response.ErrorCode != ErrorCode.NoError) {
                throw new KafkaException(response.ErrorCode);
            }
            //todo:
        }

        public void JoinGroup(String groupId) {
            var request            = new JoinGroupRequest();
            request.GroupId        = groupId;
            request.MemberId       = String.Empty;
            request.SessionTimeout = 30000;
            request.ProtocolType   = DefaultProtocolType;
            request.GroupProtocols = new[] {
                new JoinGroupRequestGroupProtocol{
                    ProtocolName     = DefaultProtocolName,
                    ProtocolMetadata = new Byte[0],
                }
            };

            var response = (JoinGroupResponse)_connection.Invoke(request);
            if (response.ErrorCode != ErrorCode.NoError) {
                throw new KafkaException(response.ErrorCode);
            }
            _groupId = groupId;
            _generationId = response.GenerationId;
            _memberId = response.MemberId;
        }

        public IList<ListGroupsResponseGroup> ListGroups() {
            var request = new ListGroupsRequest();
            var response = (ListGroupsResponse)_connection.Invoke(request);
            if (response.ErrorCode != ErrorCode.NoError) {
                throw new KafkaException(response.ErrorCode);
            }
            return response.Groups;
        }

        public DescribeGroupsResponse DescribeGroups(String groupId = null) {
            var request = new DescribeGroupsRequest();
            request.GroupId = new[] { groupId ?? _groupId };
            var response = (DescribeGroupsResponse)_connection.Invoke(request);
            var errors = response.Details.Where(x => x.ErrorCode != ErrorCode.NoError);
            if (errors.Any()) {
                throw new KafkaException(errors.First().ErrorCode);
            }
            return response;
        }

        public Byte[] SyncGroup(Byte[] memberAssignment) {
            var request = new SyncGroupRequest();
            request.GroupId = _groupId;
            request.GenerationId = _generationId;
            request.MemberId = _memberId;
            request.GroupAssignments = new[] {
                new SyncGroupRequestGroupAssignment {
                    MemberId         = _memberId,
                    MemberAssignment = memberAssignment,
                }
            };
            var response = (SyncGroupResponse)_connection.Invoke(request);
            if (response.ErrorCode != ErrorCode.NoError) {
                throw new KafkaException(response.ErrorCode);
            }
            return response.MemberAssignment;
        }

        public void Heartbeat() {
            var request = new HeartbeatRequest();
            request.GroupId = _groupId;
            request.GenerationId = _generationId;
            request.MemberId = _memberId;
            var response = (HeartbeatResponse)_connection.Invoke(request);
            if (response.ErrorCode != ErrorCode.NoError) {
                throw new KafkaException(response.ErrorCode);
            }
        }

        public void LeaveGroup() {
            var request = new LeaveGroupRequest();
            request.GroupId = _groupId;
            request.MemberId = _memberId;
            var response = (LeaveGroupResponse)_connection.Invoke(request);
            if (response.ErrorCode != ErrorCode.NoError) {
                throw new KafkaException(response.ErrorCode);
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
