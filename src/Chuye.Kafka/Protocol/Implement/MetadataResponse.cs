using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement {
    //MetadataResponse => [Broker][TopicMetadata]
    public class MetadataResponse : Response {
        public Broker[] Brokers { get; set; }
        public TopicMetadata[] TopicMetadatas { get; set; }

        protected override void DeserializeContent(BufferReader reader) {
            var brokerSize = reader.ReadInt32();
            if (brokerSize != -1) {
                Brokers = new Broker[brokerSize];
                for (int i = 0; i < Brokers.Length; i++) {
                    Brokers[i] = new Broker();
                    Brokers[i].FetchFrom(reader);
                }
            }
            var topicMetadataSize = reader.ReadInt32();
            if(topicMetadataSize!=-1)
            TopicMetadatas = new TopicMetadata[topicMetadataSize];
            {
                for (int i = 0; i < TopicMetadatas.Length; i++) {
                    TopicMetadatas[i] = new TopicMetadata();
                    TopicMetadatas[i].FetchFrom(reader);
                }
            }
        }
    }

    //Broker => NodeId Host Port  (any number of brokers may be returned)
    //  NodeId => int32
    //  Host => string
    //  Port => int32
    public class Broker : IReadable {
        public Int32 NodeId { get; set; }
        public String Host { get; set; }
        public Int32 Port { get; set; }

        public void FetchFrom(BufferReader reader) {
            NodeId = reader.ReadInt32();
            Host = reader.ReadString();
            Port = reader.ReadInt32();
        }
    }

    //TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
    //  TopicErrorCode => int16
    public class TopicMetadata : IReadable {
        //Possible Error Codes: 
        // UnknownTopic (3)
        // LeaderNotAvailable (5)
        // InvalidTopic (17)
        // TopicAuthorizationFailed (29)
        public ErrorCode TopicErrorCode { get; set; }
        public String TopicName { get; set; }
        public PartitionMetadata[] PartitionMetadatas { get; set; }

        public void FetchFrom(BufferReader reader) {
            TopicErrorCode = (ErrorCode)reader.ReadInt16();
            TopicName = reader.ReadString();
            PartitionMetadatas = new PartitionMetadata[reader.ReadInt32()];
            for (int i = 0; i < PartitionMetadatas.Length; i++) {
                PartitionMetadatas[i] = new PartitionMetadata();
                PartitionMetadatas[i].FetchFrom(reader);
            }
        }
    }

    //PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
    //  PartitionErrorCode => int16
    //  PartitionId => int32
    //  Leader => int32
    //  Replicas => [int32]
    //  Isr => [int32]  
    public class PartitionMetadata : IReadable {
        public Int16 PartitionErrorCode { get; set; }
        public Int32 PartitionId { get; set; }
        public Int32 Leader { get; set; }
        public Int32[] Replicas { get; set; }
        public Int32[] Isr { get; set; }

        public void FetchFrom(BufferReader reader) {
            PartitionErrorCode = reader.ReadInt16();
            PartitionId = reader.ReadInt32();
            Leader = reader.ReadInt32();
            Replicas = new Int32[reader.ReadInt32()];
            for (int i = 0; i < Replicas.Length; i++) {
                Replicas[i] = reader.ReadInt32();
            }
            Isr = new Int32[reader.ReadInt32()];
            for (int i = 0; i < Isr.Length; i++) {
                Isr[i] = reader.ReadInt32();
            }
        }
    }
}
