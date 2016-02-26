using System;
using System.Linq;
using System.Text;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;
using Chuye.Kafka.Protocol.Implement.Management;
using KellermanSoftware.CompareNetObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chuye.Kafka.Tests.Protocol {
    [TestClass]
    public class SerializationRequestTest {
        private readonly Random _random = new Random();

        [TestMethod]
        public void OffsetRequest() {
            var request = new OffsetRequest();
            request.ReplicaId = _random.Next();
            request.TopicPartitions = new[] {
                 new OffsetsRequestTopicPartition {
                     TopicName = Guid.NewGuid().ToString(),
                     Details   = new [] {
                         new OffsetsRequestTopicPartitionDetail() {
                             Partition          = _random.Next(),
                             Time               = (Int64)_random.Next(),
                             MaxNumberOfOffsets = _random.Next()
                         }
                     }
                 }
            };

            var bytes = new Byte[1024];
            request.Serialize(bytes, 0);
            var request2 = new OffsetRequest();
            request2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void OffsetCommitRequestV0() {
            var request = new OffsetCommitRequestV0();
            request.ConsumerGroup = Guid.NewGuid().ToString();
            request.TopicPartitions = new OffsetCommitRequestTopicPartitionV0[1];
            request.TopicPartitions = new[] {
                new OffsetCommitRequestTopicPartitionV0 {
                    TopicName = Guid.NewGuid().ToString(),
                    Details   = new [] {
                        new OffsetCommitRequestTopicPartitionDetailV0 {
                            Partition = _random.Next(),
                            Offset    = (Int64)_random.Next(),
                        }
                    }
                }
            };

            var bytes = new Byte[1024];
            request.Serialize(bytes, 0);
            var request2 = new OffsetCommitRequestV0();
            request2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void OffsetFetchRequest() {
            var request = new OffsetFetchRequest();
            request.ConsumerGroup = Guid.NewGuid().ToString();
            request.TopicPartitions = new[] {
                new OffsetFetchRequestTopicPartition {
                    TopicName  = Guid.NewGuid().ToString(),
                    Partitions = new[] { _random.Next() }
                }
            };

            var bytes = new Byte[1024];
            request.Serialize(bytes, 0);
            var request2 = new OffsetFetchRequest();
            request2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void FetchRequest() {
            var request = new FetchRequest();
            request.ReplicaId = _random.Next();
            request.MaxWaitTime = _random.Next();
            request.MinBytes = _random.Next();
            request.TopicPartitions = new[] {
                new FetchRequestTopicPartition {
                    TopicName          = Guid.NewGuid().ToString(),
                    FetchOffsetDetails = new [] {
                        new FetchRequestTopicPartitionDetail {
                            Partition   = _random.Next(),
                            FetchOffset = _random.Next(),
                            MaxBytes    = _random.Next()
                        }
                    }
                }
            };

            var bytes = new Byte[1024];
            request.Serialize(bytes, 0);
            var request2 = new FetchRequest();
            request2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void GroupCoordinatorRequest() {
            var request = new GroupCoordinatorRequest();
            request.GroupId = Guid.NewGuid().ToString();

            var bytes = new Byte[1024];
            request.Serialize(bytes, 0);
            var request2 = new GroupCoordinatorRequest();
            request2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void JoinGroupRequest() {
            var request = new JoinGroupRequest();
            request.GroupId = Guid.NewGuid().ToString();
            request.MemberId = String.Empty;
            request.SessionTimeout = 30000;
            request.ProtocolType = Guid.NewGuid().ToString();
            request.GroupProtocols = new[] {
                new JoinGroupRequestGroupProtocol{
                    ProtocolName     = Guid.NewGuid().ToString(),
                    ProtocolMetadata = new Byte[0],
                }
            };

            var bytes = new Byte[1024];
            request.Serialize(bytes, 0);
            var request2 = new JoinGroupRequest();
            request2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void ListGroupsRequest() {
            var request = new ListGroupsRequest();

            var bytes = new Byte[1024];
            request.Serialize(bytes, 0);
            var request2 = new ListGroupsRequest();
            request2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void DescribeGroupsRequest() {
            var request = new DescribeGroupsRequest();
            request.GroupId = new[] { Guid.NewGuid().ToString() };

            var bytes = new Byte[1024];
            request.Serialize(bytes, 0);
            var request2 = new DescribeGroupsRequest();
            request2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void SyncGroupRequest() {
            var request = new SyncGroupRequest();
            request.GroupId = Guid.NewGuid().ToString();
            request.GenerationId = _random.Next();
            request.MemberId = Guid.NewGuid().ToString();
            request.GroupAssignments = new[] {
                new SyncGroupRequestGroupAssignment {
                    MemberId         = Guid.NewGuid().ToString(),
                    MemberAssignment = new Byte[0],
                }
            };

            var bytes = new Byte[1024];
            request.Serialize(bytes, 0);
            var request2 = new SyncGroupRequest();
            request2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void HeartbeatRequest() {
            var request = new HeartbeatRequest();
            request.GroupId = Guid.NewGuid().ToString();
            request.GenerationId = _random.Next();
            request.MemberId = Guid.NewGuid().ToString();

            var bytes = new Byte[1024];
            request.Serialize(bytes, 0);
            var request2 = new HeartbeatRequest();
            request2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void LeaveGroupRequest() {
            var request = new LeaveGroupRequest();
            request.GroupId = Guid.NewGuid().ToString();
            request.MemberId = Guid.NewGuid().ToString();

            var bytes = new Byte[1024];
            request.Serialize(bytes, 0);
            var request2 = new LeaveGroupRequest();
            request2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void ProduceRequest() {
            var count = _random.Next(5, 10);
            var messages = Enumerable.Range(0, count)
                .Select(x => new KeyedMessage(Guid.NewGuid().ToString(), Guid.NewGuid().ToString()))
                .ToArray();
            var messageSetArray = new MessageSet[count];
            for (int i = 0; i < messageSetArray.Length; i++) {
                var messageSet = messageSetArray[i] = new MessageSet();
                messageSet.Message = new Message();
                if (messages[i].Key != null) {
                    messageSet.Message.Key = Encoding.UTF8.GetBytes(messages[i].Key);
                }
                if (messages[i].Message != null) {
                    messageSet.Message.Value = Encoding.UTF8.GetBytes(messages[i].Message);
                }
            }

            var request = new ProduceRequest();
            request.RequiredAcks = AcknowlegeStrategy.Block; //important
            request.Timeout = _random.Next();
            request.TopicPartitions = new[] {
                new ProduceRequestTopicPartition {
                    TopicName = Guid.NewGuid().ToString(),
                    Details =new [] {
                        new ProduceRequestTopicDetail {
                            Partition = _random.Next(),
                            MessageSets = new MessageSetCollection {
                                Items = messageSetArray
                            }
                        }
                    }
                }
            };

            var bytes = new Byte[4096];
            var writed = request.Serialize(bytes, 0);
            var request2 = new ProduceRequest();
            request2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.IsTrue(result.AreEqual);
        }
    }
}
