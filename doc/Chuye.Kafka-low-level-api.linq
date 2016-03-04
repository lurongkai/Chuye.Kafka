<Query Kind="Statements">
  <Reference>D:\Documents\Cloud7Git\Chuye.Kafka\release\Chuye.Kafka\Chuye.Kafka.exe</Reference>
  <Reference>&lt;RuntimeDirectory&gt;\System.Web.dll</Reference>
  <Namespace>Chuye.Kafka</Namespace>
  <Namespace>Chuye.Kafka.Protocol</Namespace>
  <Namespace>Chuye.Kafka.Protocol.Implement</Namespace>
  <Namespace>Chuye.Kafka.Protocol.Implement.Management</Namespace>
  <Namespace>System.Web</Namespace>
</Query>

//GroupCoordinatorRequest
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    const String demoConsumerGroup = "demoConsumerGroup";
    var connection = new Connection(section);
    var request = new GroupCoordinatorRequest();
    request.GroupId = "1";
    connection.Invoke(request.Dump("GroupCoordinatorRequest")).Dump("GroupCoordinatorResponse");
}

//JoinGroupRequest
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    const String demoConsumerGroup = "demoConsumerGroup";
    const String demoProtocolType = "demoProtocolType";
    const String demoProtocolName = "demoProtocolName";
    var connection = new Connection(section);
    var request = new JoinGroupRequest {
        GroupId = demoConsumerGroup,
        MemberId = "",
        SessionTimeout = 30000,
        ProtocolType = demoProtocolType,
        GroupProtocols = new[] {
            new JoinGroupRequestGroupProtocol{
                ProtocolName = demoProtocolName,
                ProtocolMetadata = new Byte[0],
            }
        }
    };
    connection.Invoke(request.Dump("JoinGroupRequest")).Dump("JoinGroupResponse");
}
//ListGroupsRequest
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9093);
    var connection = new Connection(section);
    var request = new ListGroupsRequest();
    connection.Invoke(request.Dump("ListGroupsRequest")).Dump("ListGroupsResponse");
}
//DescribeGroupsRequest
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    var connection = new Connection(section);
    var request = new DescribeGroupsRequest();
    request.GroupId = new String[] {};
    connection.Invoke(request.Dump("DescribeGroupsRequest")).Dump("DescribeGroupsResponse");
} 
//SyncGroupRequest
{
    const String demoConsumerGroup = "demoConsumerGroup";
    var memberId = "chuye.kafka-29228ff2-4121-4f98-bf82-ab999178a8db";
    var request = new SyncGroupRequest();
    request.GroupId = demoConsumerGroup;
    request.GenerationId = 1;
    request.MemberId = memberId;
    request.GroupAssignments = new[] {
                new SyncGroupRequestGroupAssignment {
                    MemberId = memberId,
                    MemberAssignment = new Byte[0],
                }
            };
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);            
    var connection = new Connection(section);
    connection.Invoke(request.Dump("SyncGroupRequest"))
        .Dump("SyncGroupResponse");
}
//HeartbeatRequest
{
    const String demoConsumerGroup = "demoConsumerGroup";
    var memberId = "chuye.kafka-29228ff2-4121-4f98-bf82-ab999178a8db";
    var request = new HeartbeatRequest();
    request.GroupId = demoConsumerGroup;
    request.GenerationId = 1;
    request.MemberId = memberId;
    
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    var connection = new Connection(section);
    connection.Invoke(request.Dump("HeartbeatRequest"))
        .Dump("HeartbeatResponse");
}
//LeaveGroupRequest
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    const String demoConsumerGroup = "demoConsumerGroup";
    var connection = new Connection(section);
    var request = new LeaveGroupRequest();
    request.GroupId = demoConsumerGroup;
    request.MemberId = "";
    connection.Invoke(request.Dump("LeaveGroupRequest")).Dump("LeaveGroupResponse");
}

//FetchRequest 
{
    var section = new KafkaConfigurationSection("jusfr.kafka", 9092);
    var connection = new Connection(section);
    //const String demoTopic = "demoTopic";
    const String demoTopic = "__consumer_offsets";
    const String demoConsumerGroup = "demoConsumerGroup";

    var request = new FetchRequest();
    request.ReplicaId = -1;
    request.MaxWaitTime = 100;
    request.MinBytes = 4096;
    request.TopicPartitions = new[] {
                new FetchRequestTopicPartition {
                    TopicName = demoTopic,
                    FetchOffsetDetails = new [] {
                        new FetchRequestTopicPartitionDetail {
                            Partition = 44,
                            FetchOffset = 0,
                            MaxBytes = 60 * 1024
                        }
                    }
                }
            };

    for (int i = 0; i < 50; i++) {
        request.TopicPartitions[0].FetchOffsetDetails[0].Partition = i;
        var response = (FetchResponse)connection.Invoke(request);
        var messages = response.TopicPartitions.SelectMany(x => x.MessageBodys)
            .SelectMany(x => x.MessageSet.Items);

        var list = new List<OffsetKeyedMessage>();
        foreach (var msg in messages) {
            var key = msg.Message.Key != null ? Encoding.UTF8.GetString(msg.Message.Key) : null;
            var message = msg.Message.Value != null ? Encoding.UTF8.GetString(msg.Message.Value) : null;
            list.Add(new OffsetKeyedMessage(msg.Offset, key, message));
        }
        if (list.Count > 0) {
            list.Dump(String.Format("Partition#" + i));
        }
    }
}