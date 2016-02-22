using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Utils;

namespace Chuye.Kafka.Protocol {

    interface IWriteable {
        void SaveTo(Writer writer);
    }

    interface IReadable {
        void FetchFrom(Reader reader);
    }

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

        public virtual Int32 Serialize(ArraySegment<Byte> bytes) {
            Verify();
            var writer = new Writer(bytes);
            SaveTo(writer);
            return writer.Count;
        }

        public virtual void SaveTo(Writer writer) {
            using (writer.PrepareLength()) {
                writer.Write((Int16)ApiKey);
                writer.Write(ApiVersion);
                writer.Write(CorrelationId);
                writer.Write(ClientId);
                SerializeContent(writer);
            }
        }

        protected abstract void SerializeContent(Writer writer);
    }
}


