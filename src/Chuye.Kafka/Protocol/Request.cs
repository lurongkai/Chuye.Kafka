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
    public abstract class Request : IWriteable, IReadable {
        private static Int32 CurrentCorrelationId;
        private const String DefaultClientId = "chuye.kafka";

        public Int32 Size { get; private set; }
        public ApiKey ApiKey { get; private set; }
        public Int16 ApiVersion { get; set; }
        public Int32 CorrelationId { get; private set; }
        public String ClientId { get; private set; }

        public Request(ApiKey apiKey) {
            ApiKey        = apiKey;
            ApiVersion    = 0;
            CorrelationId = System.Threading.Interlocked.Increment(ref CurrentCorrelationId);
            ClientId      = DefaultClientId;
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

        public void Deserialize(Byte[] bytes, Int32 offset) {
            var reader = new BufferReader(bytes, offset);
            FetchFrom(reader);
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

        public void FetchFrom(BufferReader reader) {
            Size          = reader.ReadInt32();
            var apiKey    = (ApiKey)reader.ReadInt16();
            if (ApiKey != apiKey) {
                throw new InvalidOperationException("Request type definition error");
            }
            ApiVersion    = reader.ReadInt16();
            CorrelationId = reader.ReadInt32();
            ClientId      = reader.ReadString();
            DeserializeContent(reader);
        }

        protected abstract void DeserializeContent(BufferReader reader);

    }
}


