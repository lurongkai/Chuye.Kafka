using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol {
    //FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
    //  TopicName => string
    //  Partition => int32
    //  ErrorCode => int16
    //  HighwaterMarkOffset => int64
    //  MessageSetSize => int32
    public class FetchResponse : Response {
        public FetchResponseDetail[] Items { get; set; }

        protected override void DeserializeContent(Reader reader) {
            Items = new FetchResponseDetail[reader.ReadInt32()];
            for (int i = 0; i < Items.Length; i++) {
                Items[i] = new FetchResponseDetail();
                Items[i].FetchFrom(reader);
            }
        }
    }

    public class FetchResponseDetail : IReadable {
        public String TopicName { get; set; }
        public MessageBody[] MessageBodys { get; set; }

        public void FetchFrom(Reader reader) {
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
        public MessageSetCollection MessageSets { get; set; }

        public void FetchFrom(Reader reader) {
            Partition = reader.ReadInt32();
            ErrorCode = (ErrorCode)reader.ReadInt16();
            HighwaterMarkOffset = reader.ReadInt64();
            MessageSetSize = reader.ReadInt32();
            MessageSets = new MessageSetCollection(MessageSetSize);
            MessageSets.FetchFrom(reader);
        }
    }
}
