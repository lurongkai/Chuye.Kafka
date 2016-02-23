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
            var size = reader.ReadInt32();
            TopicPartitions = new FetchResponseTopicPartition[size];
            for (int i = 0; i < TopicPartitions.Length; i++) {
                TopicPartitions[i] = new FetchResponseTopicPartition();
                TopicPartitions[i].FetchFrom(reader);
            }
        }
    }

    public class FetchResponseTopicPartition : IReadable {
        public String TopicName { get; set; }
        public MessageBody[] MessageBodys { get; set; }

        public void FetchFrom(BufferReader reader) {
            TopicName = reader.ReadString();
            var size = reader.ReadInt32();
            if (size == -1) {
                return;
            }
            MessageBodys = new MessageBody[size];
            for (int i = 0; i < MessageBodys.Length; i++) {
                MessageBodys[i] = new MessageBody();
                MessageBodys[i].FetchFrom(reader);
            }
        }
    }

    public class MessageBody : IReadable {
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
            Partition = reader.ReadInt32();
            ErrorCode = (ErrorCode)reader.ReadInt16();
            HighwaterMarkOffset = reader.ReadInt64();
            MessageSetSize = reader.ReadInt32();
            MessageSet = new MessageSetCollection(MessageSetSize);
            MessageSet.FetchFrom(reader);
        }
    }
}
