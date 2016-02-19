using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Chuye.Kafka.Protocol {
    //MessageSet => [Offset MessageSize Message]
    //  Offset => int64
    //  MessageSize => int32
    class MessageSet {
        private Int64 Offset;
        private Int32 MessageSize;
        private Message Message;
    }
    //Message => Crc MagicByte Attributes Key Value
    //  Crc => int32
    //  MagicByte => int8
    //  Attributes => int8
    //  Key => bytes
    //  Value => bytes
    class Message {
        private Int32 Crc;
        private Byte MagicByte;
        private Byte Attributes;
        private Byte[] Key;
        private Byte[] Value;
    }
}
