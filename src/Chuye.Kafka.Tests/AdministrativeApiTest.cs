using System;
using Chuye.Kafka.Protocol.Implement.Management;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Chuye.Kafka.Tests {
    [TestClass]
    public class AdministrativeApiTest {
        private readonly Connection _connection;
        private const String demoConsumerGroup = "demoConsumerGroup";

        private String _memberId;
        private Int32 _generationId;

        public AdministrativeApiTest() {
            _connection = new Connection();
        }

        [TestMethod]
        public void GroupOpreate() {
            GroupCoordinator();
            JoinGroup();
            ListGroups();
            DescribeGroups();
            SyncGroup();
            Heartbeat();
            LeaveGroup();
        }

        private void GroupCoordinator() {
            var request = new GroupCoordinatorRequest();
            request.GroupId = demoConsumerGroup;
            _connection.Invoke(request.Dump("GroupCoordinatorRequest"))
                .Dump("GroupCoordinatorResponse");
        }

        private void JoinGroup() {
            const String demoProtocolType = "demoProtocolType";
            const String demoProtocolName = "demoProtocolName";
            var request = new JoinGroupRequest {
                GroupId        = demoConsumerGroup,
                MemberId       = "",
                SessionTimeout = 30000,
                ProtocolType   = demoProtocolType,
                GroupProtocols = new[] {
                    new JoinGroupRequestGroupProtocol{
                        ProtocolName     = demoProtocolName,
                        ProtocolMetadata = new Byte[0],
                    }
                }
            };
            var response = (JoinGroupResponse)_connection.Invoke(request.Dump("JoinGroupRequest"))
                .Dump("JoinGroupResponse");
            _memberId = response.MemberId;
            _generationId = response.GenerationId;
        }

        private void ListGroups() {
            var request = new ListGroupsRequest();
            _connection.Invoke(request.Dump("ListGroupsRequest"))
                .Dump("ListGroupsResponse");
        }

        private void DescribeGroups() {
            var request = new DescribeGroupsRequest();
            request.GroupId = new[] { demoConsumerGroup };
            _connection.Invoke(request.Dump("DescribeGroupsRequest"))
                .Dump("DescribeGroupsResponse");
        }

        private void SyncGroup() {
            var request = new SyncGroupRequest();
            request.GroupId          = demoConsumerGroup;
            request.GenerationId     = _generationId;
            request.MemberId         = _memberId;
            request.GroupAssignments = new[] {
                new SyncGroupRequestGroupAssignment {
                    MemberId = _memberId,
                    MemberAssignment = new Byte[0],
                }
            };
            _connection.Invoke(request.Dump("SyncGroupRequest"))
                .Dump("SyncGroupResponse");
        }

        private void Heartbeat() {
            var request = new HeartbeatRequest();
            request.GroupId = demoConsumerGroup;
            request.GenerationId = _generationId;
            request.MemberId = _memberId;
            _connection.Invoke(request.Dump("HeartbeatRequest"))
                .Dump("HeartbeatResponse");
        }

        private void LeaveGroup() {
            var request = new LeaveGroupRequest();
            request.GroupId = demoConsumerGroup;
            request.MemberId = _memberId;
            _connection.Invoke(request.Dump("LeaveGroupRequest"))
                .Dump("LeaveGroupResponse");
        }
    }
}
