using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        public Int16 ApiKey { get; private set; }
        public Int16 ApiVersion { get; set; }
        public Int32 CorrelationId { get; private set; }
        public String ClientId { get; private set; }

        public Request(RequestApiKey apiKey) {
            ApiKey = (Int16)apiKey;
            ApiVersion = 0;
            CorrelationId = System.Threading.Interlocked.Increment(ref CurrentCorrelationId);
            ClientId = "Kafka-Net";
        }

        public virtual ArraySegment<Byte> Serialize(Byte[] bytes) {
            var writer = new Writer(bytes);
            SaveTo(writer);
            return writer.Bytes;
        }

        public virtual void SaveTo(Writer writer) {
            using (writer.PrepareLength()) {
                writer.Write(ApiKey);
                writer.Write(ApiVersion);
                writer.Write(CorrelationId);
                writer.Write(ClientId);
                SerializeContent(writer);
            }
        }

        protected abstract void SerializeContent(Writer writer);
    }
}


