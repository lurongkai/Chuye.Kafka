using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;
using Chuye.Kafka.Utils;

namespace Chuye.Kafka.Protocol {
    //RequestOrResponse => Size (RequestMessage | ResponseMessage)
    //Size => int32
    //-------------------------------------------------------------------------
    //RequestMessage => ApiKey ApiVersion CorrelationId ClientId RequestMessage
    //ApiKey => int16
    //ApiVersion => int16
    //CorrelationId => int32
    //ClientId => string
    //-------------------------------------------------------------------------
    //RequestMessage => MetadataRequest | ProduceRequest | FetchRequest | OffsetRequest | OffsetCommitRequest | OffsetFetchRequest
    public abstract class Request : IWriteable {
        private static Int32 CurrentCorrelationId;

        public Int32 Size { get; private set; }
        public ApiKey ApiKey { get; private set; }
        public Int16 ApiVersion { get; set; }
        public Int32 CorrelationId { get; private set; }
        public String ClientId { get; private set; }

        public Request(ApiKey apiKey) {
            ApiKey = apiKey;
            ApiVersion = 0;
            CorrelationId = System.Threading.Interlocked.Increment(ref CurrentCorrelationId);
            ClientId = "chuye.kafka";
        }

        protected void Verify() {
            DataAnnotationHelper.ThrowIfInvalid(this);
        }

        public Int32 Serialize(ArraySegment<Byte> buffer) {
            return Serialize(buffer.Array, buffer.Offset);
        }
        public Int32 Serialize(Byte[] bytes, Int32 offset) {
            Verify();
            var writer = new BufferWriter(bytes, offset);
            SaveTo(writer);
            return writer.Count;
        }

        public virtual void SaveTo(BufferWriter writer) {
            var compute = writer.PrepareLength();
            writer.Write((Int16)ApiKey);
            writer.Write(ApiVersion);
            writer.Write(CorrelationId);
            writer.Write(ClientId);
            SerializeContent(writer);
            compute.Dispose();
            Size = compute.Output;
        }

        protected abstract void SerializeContent(BufferWriter writer);
    }
}


