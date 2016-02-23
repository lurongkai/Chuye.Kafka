using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Chuye.Kafka.Serialization;

namespace Chuye.Kafka.Protocol {
    //MessageSet => [Offset MessageSize Message]
    //  Offset => int64
    //  MessageSize => int32
    public class MessageSetCollection : IReadable, IWriteable {
        private readonly Int32 _messageSetSize;

        public MessageSet[] Items { get; set; }

        public MessageSetCollection() {
        }

        public MessageSetCollection(Int32 messageSetSize) {
            _messageSetSize = messageSetSize;
        }

        public void FetchFrom(BufferReader reader) {
            var begin = reader.Offset;
            var sets = new List<MessageSet>();
            while (reader.Offset - begin < _messageSetSize) {
                var set = new MessageSet();
                set.FetchFrom(reader);
                sets.Add(set);
            }
            Items = sets.ToArray();
        }

        public void SaveTo(BufferWriter writer) {
            //N.B., MessageSets are not preceded by an int32 like other array elements in the protocol.
            //writer.Write(Items.Length);
            foreach (var item in Items) {
                item.SaveTo(writer);
            }
        }
    }

    public class MessageSet : IReadable, IWriteable {
        public Int64 Offset { get; set; }
        public Int32 MessageSize { get; private set; }
        public Message Message { get; set; }

        public void FetchFrom(BufferReader reader) {
            Offset = reader.ReadInt64();
            MessageSize = reader.ReadInt32();
            Message = new Message();
            Message.FetchFrom(reader);
        }

        public void SaveTo(BufferWriter writer) {
            writer.Write(Offset);
            //writer.Write(MessageSize);
            using (var compute = writer.PrepareLength()) {
                Message.SaveTo(writer);
                MessageSize = compute.Output;
            }
        }
    }
    //Message => Crc MagicByte Attributes Key Value
    //  Crc => int32
    //  MagicByte => int8
    //  Attributes => int8
    //  Key => bytes
    //  Value => bytes
    public class Message : IReadable, IWriteable {
        public Int32 Crc { get; private set; }
        public Byte MagicByte { get; set; }
        public Byte Attributes { get; set; }
        public Byte[] Key { get; set; }
        public Byte[] Value { get; set; }

        public void FetchFrom(BufferReader reader) {
            Crc = reader.ReadInt32();
            MagicByte = reader.ReadByte();
            Attributes = reader.ReadByte();
            Key = reader.ReadBytes();
            Value = reader.ReadBytes();
        }

        public void SaveTo(BufferWriter writer) {
            //writer.Write(Crc);
            using (var compute = writer.PrepareCrc()) {
                writer.Write(MagicByte);
                writer.Write(Attributes);
                writer.Write(Key);
                writer.Write(Value);
                Crc = compute.Output;
            }
        }
    }
}
