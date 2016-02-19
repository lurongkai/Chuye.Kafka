using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol.Implement {
    //ProduceRequest => RequiredAcks Timeout[TopicName[Partition MessageSetSize MessageSet]]
    //  RequiredAcks => int16
    //  Timeout => int32
    //  Partition => int32
    //  MessageSetSize => int32
    public class ProduceRequest : Request {
        public ProduceRequest()
            : base(RequestApiKey.ProduceRequest) {
        }

        public Int16 RequiredAcks { get; set; }
        public Int32 Timeout { get; set; }
        public Int32 Partition { get; set; }
        public Int32 MessageSetSize { get; set; }

        protected override void SerializeContent(Writer writer) {
            throw new NotImplementedException();
        }
    }

    //ProduceResponse => [TopicName [Partition ErrorCode Offset]]
    //  TopicName => string
    //  Partition => int32
    //  ErrorCode => int16
    //  Offset => int64
}
