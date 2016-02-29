using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement {
    //MetadataResponse => [Broker][TopicMetadata]
    public class TopicMetadataResponse : Response {
        public Broker[] Brokers { get; set; }
        public TopicMetadata[] TopicMetadatas { get; set; }

        protected override void DeserializeContent(BufferReader reader) {
            Brokers = reader.ReadArray<Broker>();
            TopicMetadatas = reader.ReadArray<TopicMetadata>();
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(Brokers);
            writer.Write(TopicMetadatas);
        }
    }

    //Broker => NodeId Host Port  (any number of brokers may be returned)
    //  NodeId => int32
    //  Host => string
    //  Port => int32
    public class Broker : IReadable, IWriteable, IEquatable<Broker> {
        public Int32 NodeId { get; set; }
        public String Host { get; set; }
        public Int32 Port { get; set; }

        public void FetchFrom(BufferReader reader) {
            NodeId = reader.ReadInt32();
            Host   = reader.ReadString();
            Port   = reader.ReadInt32();
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write(NodeId);
            writer.Write(Host);
            writer.Write(Port);
        }

        public Boolean Equals(Broker other) {
            return other != null
                && NodeId == other.NodeId
                && String.Equals(Host, other.Host, StringComparison.OrdinalIgnoreCase)
                && Port == other.Port;
        }

        public override Boolean Equals(Object obj) {
            return obj != null
                && obj is Broker
                && Equals((Broker)obj);
        }

        public override Int32 GetHashCode() {
            return NodeId.GetHashCode()
                ^ (Host != null ? Host.GetHashCode() : 0)
                ^ Port.GetHashCode();
        }
    }

    //TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
    //  TopicErrorCode => int16
    public class TopicMetadata : IReadable, IWriteable, IEquatable<TopicMetadata> {
        //Possible Error Codes: 
        // UnknownTopic (3)
        // LeaderNotAvailable (5)
        // InvalidTopic (17)
        // TopicAuthorizationFailed (29)
    public ErrorCode TopicErrorCode { get; set; }
        public String TopicName { get; set; }
        public PartitionMetadata[] PartitionMetadatas { get; set; }

        public void FetchFrom(BufferReader reader) {
            TopicErrorCode     = (ErrorCode)reader.ReadInt16();
            TopicName          = reader.ReadString();
            PartitionMetadatas = reader.ReadArray<PartitionMetadata>();
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write((Int16)TopicErrorCode);
            writer.Write(TopicName);
            writer.Write(PartitionMetadatas);
        }

        public Boolean Equals(TopicMetadata other) {
            return other != null
                && String.Equals(TopicName, other.TopicName, StringComparison.Ordinal);
        }

        public override Boolean Equals(Object obj) {
            return obj != null
                && obj is TopicMetadata
                && Equals((TopicMetadata)obj);
        }

        public override Int32 GetHashCode() {
            return TopicName.GetHashCode();
        }
    }

    //PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
    //  PartitionErrorCode => int16
    //  PartitionId => int32
    //  Leader => int32
    //  Replicas => [int32]
    //  Isr => [int32]  
    public class PartitionMetadata : IReadable, IWriteable {
        public ErrorCode PartitionErrorCode { get; set; }
        public Int32 PartitionId { get; set; }
        public Int32 Leader { get; set; }
        public Int32[] Replicas { get; set; }
        public Int32[] Isr { get; set; }

        public void FetchFrom(BufferReader reader) {
            PartitionErrorCode = (ErrorCode)reader.ReadInt16();
            PartitionId        = reader.ReadInt32();
            Leader             = reader.ReadInt32();
            Replicas           = reader.ReadInt32Array();
            Isr                = reader.ReadInt32Array();
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write((Int16)PartitionErrorCode);
            writer.Write(PartitionId);
            writer.Write(Leader);
            writer.Write(Replicas);
            writer.Write(Isr);
        }
    }
}
