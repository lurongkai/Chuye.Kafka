using System;
using System.Linq;
using System.Text;
using Chuye.Kafka.Protocol;
using Chuye.Kafka.Protocol.Implement;
using Chuye.Kafka.Protocol.Implement.Management;
using KellermanSoftware.CompareNetObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chuye.Kafka.Tests {
    [TestClass]
    public class SerializationResponseTest {
        private readonly Random _random = new Random();

        [TestMethod]
        public void OffsetResponse() {
            var response = new OffsetResponse();
            response.TopicPartitions = new[] {
                new OffsetResponseTopicPartition {
                    TopicName = Guid.NewGuid().ToString(),
                    PartitionOffsets = new [] {
                        new OffsetResponsePartitionOffset {
                            Partition = _random.Next(),
                            ErrorCode = ErrorCode.NoError,
                            Offsets   = new Int64 [] { _random.Next() }
                        }
                    }
                }
            };

            var bytes = new Byte[1024];
            response.Serialize(bytes, 0);
            var response2 = new OffsetResponse();
            response2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response, response2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void OffsetCommitResponse() {
            var response = new OffsetCommitResponse {
                TopicPartitions = new[] {
                    new OffsetCommitResponseTopicPartition {
                        TopicName = Guid.NewGuid().ToString(),
                        Details = new [] {
                            new OffsetCommitResponseTopicPartitionDetail {
                                Partition = _random.Next(),
                            }
                        }
                    }
                }
            };

            var bytes = new Byte[1024];
            response.Serialize(bytes, 0);
            var response2 = new OffsetCommitResponse();
            response2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response, response2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void OffsetFetchResponse() {
            var response = new OffsetFetchResponse {
                TopicPartitions = new[] {
                    new OffsetFetchResponseTopicPartition {
                        TopicName = Guid.NewGuid().ToString(),
                        Details = new [] {
                            new OffsetFetchResponseTopicPartitionDetail {
                                Partition = _random.Next(),
                                Metadata  = Guid.NewGuid().ToString(),
                                Offset    = (Int64)_random.Next(),
                            }
                        }
                    }
                }
            };

            var bytes = new Byte[1024];
            response.Serialize(bytes, 0);
            var response2 = new OffsetFetchResponse();
            response2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response, response2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void FetchResponse() {
            var response = new FetchResponse {
                TopicPartitions = new[] {
                    new FetchResponseTopicPartition {
                        TopicName = Guid.NewGuid().ToString(),
                        MessageBodys = new [] {
                            new MessageBody {
                                Partition           = _random.Next(),
                                HighwaterMarkOffset = (Int64)_random.Next(),
                                //MessageSetSize      = _random.Next(),
                                //todo: MessageSetCollection 的断言需要另外进行
                                MessageSet          = new MessageSetCollection {
                                    Items = new MessageSet[0] {
                                        //new MessageSet {
                                        //    Offset = (Int64)_random.Next(),
                                        //    //MessageSize = _random.Next(),
                                        //    Message = new Message {
                                        //        //Crc = _random.Next(),
                                        //        MagicByte  = (Byte)_random.Next(Byte.MaxValue),
                                        //        Attributes = (Byte)_random.Next(Byte.MaxValue),
                                        //        Key        = new Byte[0],
                                        //        Value      = new Byte[0]
                                        //    }
                                        //}
                                    }
                                }
                            }
                        }
                    }
                }
            };

            var bytes = new Byte[40960];
            var writed = response.Serialize(bytes, 0);
            var response2 = new FetchResponse();
            response2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response, response2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void GroupCoordinatorResponse() {
            var response = new GroupCoordinatorResponse {
                //CorrelationId = _random.Next(),
                CoordinatorHost = Guid.NewGuid().ToString(),
                CoordinatorPort = _random.Next(),
            };

            var bytes = new Byte[1024];
            response.Serialize(bytes, 0);
            var response2 = new GroupCoordinatorResponse();
            response2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response, response2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void JoinGroupResponse() {
            var response = new JoinGroupResponse {
                GenerationId = _random.Next(),
                GroupProtocol = Guid.NewGuid().ToString(),
                LeaderId = Guid.NewGuid().ToString(),
                MemberId = Guid.NewGuid().ToString(),
                Members = new[] {
                    new JoinGroupResponseMember {
                        MemberId = Guid.NewGuid().ToString(),
                        MemberMetadata = new Byte[0],
                    }
                }
            };

            var bytes = new Byte[1024];
            response.Serialize(bytes, 0);
            var response2 = new JoinGroupResponse();
            response2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response, response2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void ListGroupsResponse() {
            var response = new ListGroupsResponse {
                Groups = new[] {
                    new ListGroupsResponseGroup {
                        GroupId = Guid.NewGuid().ToString(),
                        ProtocolType = Guid.NewGuid().ToString(),
                    }
                }
            };

            var bytes = new Byte[1024];
            response.Serialize(bytes, 0);
            var response2 = new ListGroupsResponse();
            response2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response, response2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void DescribeGroupsResponse() {
            var response = new DescribeGroupsResponse {
                Details = new[] {
                    new DescribeGroupsResponseDetail {
                        GroupId      = Guid.NewGuid().ToString(),
                        State        = Guid.NewGuid().ToString(),
                        ProtocolType = Guid.NewGuid().ToString(),
                        Protocol     = Guid.NewGuid().ToString(),
                        Members      = new [] {
                            new DescribeGroupsResponseMember {
                                MemberId         = Guid.NewGuid().ToString(),
                                ClientId         = Guid.NewGuid().ToString(),
                                ClientHost       = Guid.NewGuid().ToString(),
                                MemberMetadata   = new Byte[0],
                                MemberAssignment = new Byte[0],
                            }
                        }
                    }
                }
            };

            var bytes = new Byte[1024];
            response.Serialize(bytes, 0);
            var response2 = new DescribeGroupsResponse();
            response2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response, response2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void SyncGroupResponse() {
            var response = new SyncGroupResponse {
                MemberAssignment = new Byte[Math.Abs(_random.Next(Byte.MaxValue))],
            };

            var bytes = new Byte[1024];
            response.Serialize(bytes, 0);
            var response2 = new SyncGroupResponse();
            response2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response, response2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void HeartbeatResponse() {
            var request = new HeartbeatResponse();

            var bytes = new Byte[1024];
            request.Serialize(bytes, 0);
            var request2 = new HeartbeatResponse();
            request2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void LeaveGroupResponse() {
            var request = new LeaveGroupResponse();

            var bytes = new Byte[1024];
            request.Serialize(bytes, 0);
            var request2 = new LeaveGroupResponse();
            request2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(request, request2);
            Assert.IsTrue(result.AreEqual);
        }

        [TestMethod]
        public void ProduceResponse() {
            var response = new ProduceResponse {
                TopicPartitions = new[] {
                    new ProduceResponseTopicPartition {
                        TopicName = Guid.NewGuid().ToString(),
                        Details = new [] {
                            new ProduceResponseTopicPartitionDetail {
                                Partition = _random.Next(),
                                Offset = (Int64)_random.Next(),
                            }
                        }
                    }
                }
            };

            var bytes = new Byte[1024];
            response.Serialize(bytes, 0);
            var response2 = new ProduceResponse();
            response2.Deserialize(bytes, 0);

            var compareLogic = new CompareLogic();
            var result = compareLogic.Compare(response, response2);
            Assert.IsTrue(result.AreEqual);
        }
    }
}
