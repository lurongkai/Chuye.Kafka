using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement {
    //MetadataResponse => [Broker][TopicMetadata]
    class MetadataResponse : Response {
        public Broker[] Brokers { get; set; }
        public TopicMetadata[] TopicMetadatas { get; set; }

        protected override void DeserializeContent(Reader reader) {
            Brokers = new Broker[reader.ReadInt32()];
            for (int i = 0; i < Brokers.Length; i++) {
                Brokers[i] = new Broker();
                Brokers[i].Read(reader);
            }
            TopicMetadatas = new TopicMetadata[reader.ReadInt32()];
            for (int i = 0; i < TopicMetadatas.Length; i++) {
                TopicMetadatas[i] = new TopicMetadata();
                TopicMetadatas[i].Read(reader);
            }
        }
    }

    //Broker => NodeId Host Port  (any number of brokers may be returned)
    //  NodeId => int32
    //  Host => string
    //  Port => int32
    class Broker : IReadable {
        public Int32 NodeId { get; set; }
        public String Host { get; set; }
        public Int32 Port { get; set; }

        public void Read(Reader reader) {
            NodeId = reader.ReadInt32();
            Host = reader.ReadString();
            Port = reader.ReadInt32();
        }
    }

    //TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
    //  TopicErrorCode => int16
    class TopicMetadata : IReadable {
        //Possible Error Codes: 
        // UnknownTopic (3)
        // LeaderNotAvailable (5)
        // InvalidTopic (17)
        // TopicAuthorizationFailed (29)
        public ErrorCode TopicErrorCode { get; set; }
        public String TopicName { get; set; }
        public PartitionMetadata[] PartitionMetadatas { get; set; }

        public void Read(Reader reader) {
            TopicErrorCode = (ErrorCode)reader.ReadInt16();
            TopicName = reader.ReadString();
            PartitionMetadatas = new PartitionMetadata[reader.ReadInt32()];
            for (int i = 0; i < PartitionMetadatas.Length; i++) {
                PartitionMetadatas[i] = new PartitionMetadata();
                PartitionMetadatas[i].Read(reader);
            }
        }
    }

    //PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
    //  PartitionErrorCode => int16
    //  PartitionId => int32
    //  Leader => int32
    //  Replicas => [int32]
    //  Isr => [int32]  
    class PartitionMetadata : IReadable {
        public Int16 PartitionErrorCode { get; set; }
        public Int32 PartitionId { get; set; }
        public Int32 Leader { get; set; }
        public Int32[] Replicas { get; set; }
        public Int32[] Isr { get; set; }

        public void Read(Reader reader) {
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
