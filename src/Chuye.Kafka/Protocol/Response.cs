using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol {
    //RequestOrResponse => Size (RequestMessage | ResponseMessage)
    //Size => int32
    //-------------------------------------------------------------------------
    //Response => CorrelationId ResponseMessage
    //CorrelationId => int32
    //-------------------------------------------------------------------------
    //ResponseMessage => MetadataResponse | ProduceResponse | FetchResponse | OffsetResponse | OffsetCommitResponse | OffsetFetchResponse
    public abstract class Response {
        public Int32 Size;
        public Int32 CorrelationId;

        public void Read(ArraySegment<Byte> buffer) {
            Read(buffer.Array, buffer.Offset);
        }

        public void Read(Byte[] bytes, Int32 offset) {
            var reader = new BufferReader(bytes, offset);
            Size = reader.ReadInt32();
            CorrelationId = reader.ReadInt32();
            DeserializeContent(reader);
        }

        protected abstract void DeserializeContent(BufferReader reader);
    }
}
