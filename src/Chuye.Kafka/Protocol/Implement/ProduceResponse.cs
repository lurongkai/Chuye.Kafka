using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement {
    //ProduceResponse => [TopicName [Partition ErrorCode Offset]]
    //  TopicName => string
    //  Partition => int32
    //  ErrorCode => int16
    //  Offset => int64
    public class ProduceResponse : Response {
        public ProduceResponseDetail[] Items { get; set; }

        protected override void DeserializeContent(Reader reader) {
            Items = new ProduceResponseDetail[reader.ReadInt32()];
            for (int i = 0; i < Items.Length; i++) {
                Items[i] = new ProduceResponseDetail();
                Items[i].FetchFrom(reader);
            }
        }
    }

    public class ProduceResponseDetail : IReadable {
        public String TopicName { get; set; }
        public OffsetDetail[] Offsets { get; set; }

        public void FetchFrom(Reader reader) {
            TopicName = reader.ReadString();
            Offsets = new OffsetDetail[reader.ReadInt32()];
            for (int i = 0; i < Offsets.Length; i++) {
                Offsets[i] = new OffsetDetail();
                Offsets[i].FetchFrom(reader);
            }
        }
    }

    public class OffsetDetail : IReadable {
        public Int32 Partition { get; set; }
        //Possible Error Codes: (TODO)
        public ErrorCode ErrorCode { get; set; }
        public Int64 Offset { get; set; }

        public void FetchFrom(Reader reader) {
            Partition = reader.ReadInt32();
            ErrorCode = (ErrorCode)reader.ReadInt16();
            Offset = reader.ReadInt64();
        }
    }
}
