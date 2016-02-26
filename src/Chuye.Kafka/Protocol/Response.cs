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
        public Int32 Size { get; private set; }
        public Int32 CorrelationId { get; private set; }

        public void Deserialize(ArraySegment<Byte> buffer) {
            Deserialize(buffer.Array, buffer.Offset);
        }

        public void Deserialize(Byte[] bytes, Int32 offset) {
            var reader    = new BufferReader(bytes, offset);
            Size          = reader.ReadInt32();
            CorrelationId = reader.ReadInt32();
            DeserializeContent(reader);
        }

        public Int32 Serialize(Byte[] bytes, Int32 offset) {
            var writer  = new BufferWriter(bytes, offset);
            var compute = writer.PrepareLength();
            writer.Write(CorrelationId);
            SerializeContent(writer);
            compute.Dispose();
            Size = compute.Output;
            return writer.Count;
        }

        protected abstract void DeserializeContent(BufferReader reader);

        protected abstract void SerializeContent(BufferWriter writer);
    }
}
