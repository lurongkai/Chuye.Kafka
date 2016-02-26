using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol.Implement {
    //FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
    //  TopicName => string
    //  Partition => int32
    //  ErrorCode => int16
    //  HighwaterMarkOffset => int64
    //  MessageSetSize => int32
    public class FetchResponse : Response {
        public FetchResponseTopicPartition[] TopicPartitions { get; set; }

        protected override void DeserializeContent(BufferReader reader) {
            TopicPartitions = reader.ReadArray<FetchResponseTopicPartition>();
        }

        protected override void SerializeContent(BufferWriter writer) {
            writer.Write(TopicPartitions);
        }
    }

    public class FetchResponseTopicPartition : IReadable, IWriteable {
        public String TopicName { get; set; }
        public MessageBody[] MessageBodys { get; set; }

        public void FetchFrom(BufferReader reader) {
            TopicName    = reader.ReadString();
            MessageBodys = reader.ReadArray<MessageBody>();
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write(TopicName);
            writer.Write(MessageBodys);
        }
    }

    public class MessageBody : IReadable, IWriteable {
        public Int32 Partition { get; set; }
        //Possible Error Codes
        //* OFFSET_OUT_OF_RANGE (1)
        //* UNKNOWN_TOPIC_OR_PARTITION (3)
        //* NOT_LEADER_FOR_PARTITION (6)
        //* REPLICA_NOT_AVAILABLE (9)
        //* UNKNOWN (-1)
        public ErrorCode ErrorCode { get; set; }
        public Int64 HighwaterMarkOffset { get; set; }
        public Int32 MessageSetSize { get; set; }
        public MessageSetCollection MessageSet { get; set; }

        public void FetchFrom(BufferReader reader) {
            Partition           = reader.ReadInt32();
            ErrorCode           = (ErrorCode)reader.ReadInt16();
            HighwaterMarkOffset = reader.ReadInt64();
            MessageSetSize      = reader.ReadInt32();
            MessageSet          = new MessageSetCollection(MessageSetSize);
            MessageSet.FetchFrom(reader);
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write(Partition);
            writer.Write((Int16)ErrorCode);
            writer.Write(HighwaterMarkOffset);
            writer.Write(MessageSetSize);
            MessageSet.SaveTo(writer);
        }
    }
}
